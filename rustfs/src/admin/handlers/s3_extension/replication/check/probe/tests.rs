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

use super::super::tests::replication_check_target;
use super::*;

#[derive(Default)]
struct ScriptedReplicationProbe {
    put_error: Option<&'static str>,
    /// Version id the scripted target answers with on PUT; None models a
    /// mirroring target that echoes the sent source version id.
    minted_version_id: Option<&'static str>,
    /// Same, for the multipart leg: a target may mirror PutObject ids and
    /// still mint its own at CreateMultipartUpload.
    minted_multipart_version_id: Option<&'static str>,
    /// Transport failure of the SSE-C passthrough probe itself.
    ssec_probe_error: Option<&'static str>,
    /// Models a MinIO-like target that drops the SSE-C passthrough
    /// headers: the probe HEAD-back echoes no SSE-C evidence. The default
    /// (false) models a RustFS target that preserves them.
    ssec_evidence_missing: bool,
    delete_marker_error: Option<&'static str>,
    version_delete_error: Option<&'static str>,
    cleanup_error: Option<&'static str>,
    calls: Vec<&'static str>,
    /// Version ids the delete-marker and version-delete phases addressed.
    mutation_ids: Vec<Option<String>>,
    cleanup_ids: Vec<Option<String>>,
}

fn scripted_probe_error(code: &str) -> S3ClientError {
    S3ClientError::with_metadata(
        format!("{code}: secret remote detail"),
        None,
        Some(code.to_string()),
        Some("secret remote detail".to_string()),
    )
}

#[async_trait::async_trait]
impl ReplicationProbeOperations for ScriptedReplicationProbe {
    async fn put(&mut self) -> Result<ReplicationProbePutOutcome, S3ClientError> {
        self.calls.push("put");
        match self.put_error {
            Some(code) => Err(scripted_probe_error(code)),
            None => Ok(ReplicationProbePutOutcome {
                sent_version_id: "object-version".to_string(),
                response_version_id: Some(self.minted_version_id.unwrap_or("object-version").to_string()),
            }),
        }
    }

    async fn multipart_put(&mut self) -> Result<ReplicationProbePutOutcome, ReplicationProbeMultipartError> {
        self.calls.push("multipart-put");
        Ok(ReplicationProbePutOutcome {
            sent_version_id: "multipart-version".to_string(),
            response_version_id: Some(self.minted_multipart_version_id.unwrap_or("multipart-version").to_string()),
        })
    }

    async fn ssec_passthrough_probe(&mut self) -> Result<ReplicationSsecProbeOutcome, S3ClientError> {
        self.calls.push("ssec-probe");
        match self.ssec_probe_error {
            Some(code) => Err(scripted_probe_error(code)),
            None => Ok(ReplicationSsecProbeOutcome {
                evidence_present: !self.ssec_evidence_missing,
                version_id: Some("ssec-version".to_string()),
            }),
        }
    }

    async fn create_delete_marker(&mut self, version_id: Option<&str>) -> Result<Option<String>, S3ClientError> {
        self.calls.push("delete-marker");
        self.mutation_ids.push(version_id.map(ToOwned::to_owned));
        match self.delete_marker_error {
            Some(code) => Err(scripted_probe_error(code)),
            None => Ok(Some("marker-version".to_string())),
        }
    }

    async fn delete_version(&mut self, version_id: Option<&str>) -> Result<(), S3ClientError> {
        self.calls.push("version-delete");
        self.mutation_ids.push(version_id.map(ToOwned::to_owned));
        match self.version_delete_error {
            Some(code) => Err(scripted_probe_error(code)),
            None => Ok(()),
        }
    }

    async fn cleanup(&mut self, known_version_ids: [Option<&str>; 4]) -> Result<(), String> {
        self.calls.push("cleanup");
        self.cleanup_ids = known_version_ids
            .into_iter()
            .map(|version_id| version_id.map(ToOwned::to_owned))
            .collect();
        match self.cleanup_error {
            Some(error) => Err(error.to_string()),
            None => Ok(()),
        }
    }
}

/// P1-19: a target that mints its own version ids must fail the
/// VersionFidelity phase with the machine-readable mismatch code. The
/// mutation phases still run, addressing the id the target assigned
/// (the ledger the worker records per object, rustfs/backlog#2340), and
/// cleanup uses that id too — the source-derived id would never match.
#[tokio::test]
async fn replication_probe_flags_version_minting_target_and_probes_mutations_by_assigned_id() {
    let mut result = replication_check_target("arn:a", "OK", None);
    let mut operations = ScriptedReplicationProbe {
        minted_version_id: Some("target-minted-version"),
        ..Default::default()
    };

    execute_replication_probe(&mut result, &mut operations).await;

    assert_eq!(operations.calls, ["put", "delete-marker", "version-delete", "cleanup"]);
    assert_eq!(result.status, "FAILED");
    assert_eq!(result.phases.put.status, "OK");
    assert_eq!(result.phases.version_fidelity.status, "FAILED");
    assert_eq!(result.phases.version_fidelity.code, Some(REPLICATION_CHECK_CODE_VERSION_MISMATCH));
    assert_eq!(result.phases.delete_marker.status, "OK");
    assert_eq!(result.phases.version_delete.status, "OK");
    assert_eq!(
        operations.mutation_ids,
        [
            Some("target-minted-version".to_string()),
            Some("target-minted-version".to_string())
        ],
        "both mutation phases must address the id the target assigned"
    );
    assert_eq!(result.phases.ssec_passthrough.status, "SKIPPED");
    assert_eq!(result.phases.cleanup.status, "OK");
    assert_eq!(
        operations.cleanup_ids,
        [
            Some("target-minted-version".to_string()),
            None,
            None,
            Some("marker-version".to_string())
        ]
    );
}

/// A drifting target that also refuses the ledger-addressed delete keeps
/// the phase-level evidence: VersionDelete fails on its own, apart from
/// the identity verdict.
#[tokio::test]
async fn replication_probe_reports_version_delete_failure_on_a_drifting_target() {
    let mut result = replication_check_target("arn:a", "OK", None);
    let mut operations = ScriptedReplicationProbe {
        minted_version_id: Some("target-minted-version"),
        version_delete_error: Some("AccessDenied"),
        ..Default::default()
    };

    execute_replication_probe(&mut result, &mut operations).await;

    assert_eq!(result.phases.version_fidelity.status, "FAILED");
    assert_eq!(result.phases.delete_marker.status, "OK");
    assert_eq!(result.phases.version_delete.status, "FAILED");
    assert_eq!(result.phases.cleanup.status, "OK");
}

#[tokio::test]
async fn replication_probe_passes_version_fidelity_for_mirroring_target() {
    let mut result = replication_check_target("arn:a", "OK", None);
    let mut operations = ScriptedReplicationProbe::default();

    execute_replication_probe(&mut result, &mut operations).await;

    assert_eq!(result.status, "OK");
    assert_eq!(result.phases.version_fidelity.status, "OK");
    assert_eq!(result.phases.version_fidelity.code, None);
    assert_eq!(result.phases.ssec_passthrough.status, "OK");
    assert_eq!(result.phases.ssec_passthrough.code, None);
}

/// N2: a target that drops the SSE-C passthrough transport headers must
/// fail the SsecPassthrough phase with the machine-readable code while the
/// target overall stays OK — deliberately unlike VersionFidelity: this is
/// a capability limit, not a broken replication contract, and a
/// plaintext-only deployment against such a target must not turn red. The
/// other mutation phases keep running and the probe version is cleaned up.
#[tokio::test]
async fn replication_probe_flags_ssec_passthrough_dropping_target_without_failing_target() {
    let mut result = replication_check_target("arn:a", "OK", None);
    let mut operations = ScriptedReplicationProbe {
        ssec_evidence_missing: true,
        ..Default::default()
    };

    execute_replication_probe(&mut result, &mut operations).await;

    assert_eq!(
        operations.calls,
        [
            "put",
            "multipart-put",
            "ssec-probe",
            "delete-marker",
            "version-delete",
            "cleanup"
        ]
    );
    assert_eq!(result.status, "OK", "a capability-only failure must not fail the target overall");
    assert_eq!(result.error, None);
    assert_eq!(result.phases.ssec_passthrough.status, "FAILED");
    assert_eq!(result.phases.ssec_passthrough.code, Some(REPLICATION_CHECK_CODE_SSEC_PASSTHROUGH));
    assert_eq!(
        operations.cleanup_ids,
        [
            Some("object-version".to_string()),
            Some("multipart-version".to_string()),
            Some("ssec-version".to_string()),
            Some("marker-version".to_string())
        ]
    );
}

/// A transport failure of the SSE-C probe is not evidence of a dropping
/// target: the phase fails without the capability code (the runtime cache
/// stays Unknown and the worker keeps auditing), and the target overall
/// stays OK.
#[tokio::test]
async fn replication_probe_ssec_transport_failure_carries_no_capability_code() {
    let mut result = replication_check_target("arn:a", "OK", None);
    let mut operations = ScriptedReplicationProbe {
        ssec_probe_error: Some("InternalError"),
        ..Default::default()
    };

    execute_replication_probe(&mut result, &mut operations).await;

    assert_eq!(result.status, "OK");
    assert_eq!(result.phases.ssec_passthrough.status, "FAILED");
    assert_eq!(result.phases.ssec_passthrough.code, None);
    assert_eq!(operations.cleanup_ids[2], None, "a failed ssec probe leaves no version to clean");
}

#[tokio::test]
async fn replication_probe_attempts_cleanup_after_ambiguous_put_failure() {
    let mut result = replication_check_target("arn:a", "OK", None);
    let mut operations = ScriptedReplicationProbe {
        put_error: Some("InternalError"),
        ..Default::default()
    };

    execute_replication_probe(&mut result, &mut operations).await;

    assert_eq!(operations.calls, ["put", "cleanup"]);
    assert_eq!(result.status, "FAILED");
    assert_eq!(result.phases.put.status, "FAILED");
    assert_eq!(result.phases.delete_marker.status, "SKIPPED");
    assert_eq!(result.phases.version_delete.status, "SKIPPED");
    assert_eq!(result.phases.cleanup.status, "OK");
}

#[tokio::test]
async fn replication_probe_continues_version_delete_and_cleanup_after_marker_failure() {
    let mut result = replication_check_target("arn:a", "OK", None);
    let mut operations = ScriptedReplicationProbe {
        delete_marker_error: Some("AccessDenied"),
        ..Default::default()
    };

    execute_replication_probe(&mut result, &mut operations).await;

    assert_eq!(
        operations.calls,
        [
            "put",
            "multipart-put",
            "ssec-probe",
            "delete-marker",
            "version-delete",
            "cleanup"
        ]
    );
    assert_eq!(
        operations.cleanup_ids,
        [
            Some("object-version".to_string()),
            Some("multipart-version".to_string()),
            Some("ssec-version".to_string()),
            None
        ]
    );
    assert_eq!(result.phases.delete_marker.status, "FAILED");
    assert_eq!(result.phases.version_delete.status, "OK");
    assert_eq!(result.phases.cleanup.status, "OK");
}

#[tokio::test]
async fn replication_probe_reports_version_delete_and_cleanup_failures_separately() {
    let mut result = replication_check_target("arn:a", "OK", None);
    let mut operations = ScriptedReplicationProbe {
        version_delete_error: Some("AccessDenied"),
        cleanup_error: Some("cleanup unavailable"),
        ..Default::default()
    };

    execute_replication_probe(&mut result, &mut operations).await;

    assert_eq!(
        operations.calls,
        [
            "put",
            "multipart-put",
            "ssec-probe",
            "delete-marker",
            "version-delete",
            "cleanup"
        ]
    );
    assert_eq!(
        operations.cleanup_ids,
        [
            Some("object-version".to_string()),
            Some("multipart-version".to_string()),
            Some("ssec-version".to_string()),
            Some("marker-version".to_string())
        ]
    );
    assert_eq!(result.phases.version_delete.status, "FAILED");
    assert_eq!(result.phases.cleanup.status, "FAILED");
    assert_eq!(result.phases.cleanup.error.as_deref(), Some("cleanup unavailable"));
    assert!(
        result.error.as_deref().unwrap_or_default().contains("permissions missing"),
        "the first mutation failure remains the target summary"
    );
}

#[tokio::test]
async fn replication_probe_does_not_clean_up_a_conditional_collision() {
    let mut result = replication_check_target("arn:a", "OK", None);
    let mut operations = ScriptedReplicationProbe {
        put_error: Some("PreconditionFailed"),
        ..Default::default()
    };

    execute_replication_probe(&mut result, &mut operations).await;

    assert_eq!(operations.calls, ["put"]);
    assert_eq!(result.phases.put.status, "FAILED");
    assert_eq!(result.phases.cleanup.status, "OK");
}

#[test]
fn replication_probe_keys_use_reserved_high_entropy_namespace() {
    let first = new_replication_probe_key();
    let second = new_replication_probe_key();

    assert!(first.starts_with(REPLICATION_CHECK_PROBE_PREFIX));
    assert_eq!(
        first
            .strip_prefix(REPLICATION_CHECK_PROBE_PREFIX)
            .expect("prefix should exist")
            .split('/')
            .count(),
        2
    );
    assert_ne!(first, second);
}

#[test]
fn build_replication_probe_put_options_sets_replication_flags() {
    let now = OffsetDateTime::from_unix_timestamp(42).expect("timestamp should build");
    let options = build_replication_probe_put_options(now);

    assert_eq!(options.internal.replication_status, ReplicationStatusType::Replica);
    assert!(options.internal.replication_request);
    assert!(options.internal.replication_validity_check);
    assert_eq!(options.internal.source_mtime, now);
    assert!(!options.internal.source_version_id.is_empty());
}

#[test]
fn build_replication_probe_remove_options_sets_replication_flags() {
    let now = OffsetDateTime::from_unix_timestamp(42).expect("timestamp should build");
    let options = build_replication_probe_remove_options(now, true);

    assert!(options.replication_delete_marker);
    assert_eq!(options.replication_status, ReplicationStatusType::Replica);
    assert!(options.replication_request);
    assert!(options.replication_validity_check);
    assert_eq!(options.replication_mtime, Some(now));
}
