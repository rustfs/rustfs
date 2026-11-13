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

//! `GET /{bucket}?replication-check`: validates every replication target of a
//! bucket by actively replicating probe objects to it.
//!
//! Responsible for: selecting the targets the replication rules name, the
//! per-target checks and the JSON report. The probe mutations themselves live
//! in `probe`.
//!
//! Not responsible for: admission or the versioning precondition, which the
//! route handler in `super` runs first.

mod probe;
#[cfg(test)]
mod tests;

use crate::admin::operation::AdminResponse;
use crate::admin::runtime_sources::current_deployment_id;
use crate::admin::storage_api::bucket::AdminReplicationConfigExt as _;
use crate::admin::storage_api::bucket::metadata_sys;
use crate::admin::storage_api::bucket::target::{BucketTarget, BucketTargetType, BucketTargets};
use crate::admin::storage_api::bucket::target_sys::{
    BucketTargetSys, S3ClientError, SsecPassthroughCapability, TargetClient, VersionIdentityCapability,
};
use crate::admin::storage_api::bucket::versioning_sys::BucketVersioningSys;
use crate::admin::storage_api::error::StorageError;
use crate::admin::utils::owned_json_response;
use crate::error::ApiError;
use http::StatusCode;
use probe::{
    RemoteReplicationProbeOperations, allocate_replication_probe_key, execute_replication_probe,
    resolve_replication_target_client,
};
use rustfs_gateway_types::dto::Status;
use rustfs_gateway_types::persistence::PersistedReplicationConfiguration;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::collections::HashSet;
use time::OffsetDateTime;

const REPLICATION_CHECK_PROBE_PREFIX: &str = ".rustfs.sys/replication-check/";

const REPLICATION_CHECK_ERROR_MAX_BYTES: usize = 512;

/// RustFS extension code (no madmin analogue): the target does not adopt the
/// source version id, breaking the version-identity replication contract.
const REPLICATION_CHECK_CODE_VERSION_MISMATCH: &str = "BucketRemoteTargetVersionMismatch";

/// RustFS extension code (no madmin analogue): the target drops the
/// `X-Rustfs-Replication-*` SSE-C passthrough headers, so an SSE-C replica
/// would lose its decryption material (N2 fail-closed).
const REPLICATION_CHECK_CODE_SSEC_PASSTHROUGH: &str = "BucketRemoteSsecPassthroughUnsupported";

/// Syntactically valid stand-in SSE-C key MD5 for the passthrough probe (the
/// probe object is never decrypted; it only has to round-trip the metadata).
const REPLICATION_CHECK_SSEC_PROBE_KEY_MD5: &str = "AAAAAAAAAAAAAAAAAAAAAA==";

#[derive(Debug, Clone, serde::Serialize)]
struct ReplicationCheckResponse {
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "ActiveMutation")]
    active_mutation: bool,
    #[serde(rename = "MutationDescription")]
    mutation_description: &'static str,
    #[serde(rename = "ProbeNamespace")]
    probe_namespace: &'static str,
    #[serde(rename = "Targets")]
    targets: Vec<ReplicationCheckTargetStatus>,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationCheckTargetStatus {
    #[serde(rename = "Arn")]
    arn: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(rename = "Phases")]
    phases: ReplicationCheckPhases,
}

#[derive(Debug, Clone, serde::Serialize, Default)]
struct ReplicationCheckPhases {
    #[serde(rename = "Bucket")]
    bucket: ReplicationCheckPhaseStatus,
    #[serde(rename = "Versioning")]
    versioning: ReplicationCheckPhaseStatus,
    #[serde(rename = "ObjectLock")]
    object_lock: ReplicationCheckPhaseStatus,
    #[serde(rename = "Put")]
    put: ReplicationCheckPhaseStatus,
    #[serde(rename = "VersionFidelity")]
    version_fidelity: ReplicationCheckPhaseStatus,
    #[serde(rename = "SsecPassthrough")]
    ssec_passthrough: ReplicationCheckPhaseStatus,
    #[serde(rename = "DeleteMarker")]
    delete_marker: ReplicationCheckPhaseStatus,
    #[serde(rename = "VersionDelete")]
    version_delete: ReplicationCheckPhaseStatus,
    #[serde(rename = "Cleanup")]
    cleanup: ReplicationCheckPhaseStatus,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ReplicationCheckPhaseStatus {
    #[serde(rename = "Status")]
    status: &'static str,
    #[serde(rename = "Error", skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    /// Machine-readable failure code (RustFS extension key; Go decoders
    /// ignore unknown keys). Only set for failures that a caller is expected
    /// to branch on, e.g. `BucketRemoteTargetVersionMismatch`.
    #[serde(rename = "Code", skip_serializing_if = "Option::is_none")]
    code: Option<&'static str>,
}

impl Default for ReplicationCheckPhaseStatus {
    fn default() -> Self {
        Self {
            status: "SKIPPED",
            error: None,
            code: None,
        }
    }
}

impl ReplicationCheckPhaseStatus {
    fn passed() -> Self {
        Self {
            status: "OK",
            error: None,
            code: None,
        }
    }

    fn failed(error: impl Into<String>) -> Self {
        Self {
            status: "FAILED",
            error: Some(bound_replication_check_error(error.into())),
            code: None,
        }
    }

    fn failed_with_code(error: impl Into<String>, code: &'static str) -> Self {
        Self {
            code: Some(code),
            ..Self::failed(error)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicationCheckFailureContext {
    BucketCheck,
    VersioningCheck,
    ReplicateObject,
    ReplicateDeleteMarker,
    DeleteObjectVersion,
    ObjectLockCheck,
}

fn build_replication_check_response(mut targets: Vec<ReplicationCheckTargetStatus>) -> S3Result<AdminResponse> {
    targets.sort_by(|left, right| left.arn.cmp(&right.arn));
    let status = if targets.iter().all(|target| target.status == "OK") {
        "OK"
    } else {
        "FAILED"
    };
    owned_json_response(
        StatusCode::OK,
        &ReplicationCheckResponse {
            status: status.to_string(),
            active_mutation: true,
            mutation_description: "Writes probe objects (including an SSE-C passthrough probe), creates a delete marker, deletes the probe versions, and cleans up all probe artifacts on each target.",
            probe_namespace: REPLICATION_CHECK_PROBE_PREFIX,
            targets,
        },
    )
}

fn bound_replication_check_error(mut error: String) -> String {
    error = error.replace(['\r', '\n'], " ");
    if error.len() <= REPLICATION_CHECK_ERROR_MAX_BYTES {
        return error;
    }
    let mut end = REPLICATION_CHECK_ERROR_MAX_BYTES.saturating_sub(3);
    while !error.is_char_boundary(end) {
        end -= 1;
    }
    error.truncate(end);
    error.push_str("...");
    error
}

fn format_replication_check_client_error(err: &S3ClientError, context: ReplicationCheckFailureContext) -> String {
    if err.code.as_deref() == Some("AccessDenied") {
        return match context {
            ReplicationCheckFailureContext::ReplicateObject => {
                "s3:ReplicateObject permissions missing for replication user".to_string()
            }
            ReplicationCheckFailureContext::ReplicateDeleteMarker => {
                "s3:ReplicateDelete permissions missing for replication user".to_string()
            }
            ReplicationCheckFailureContext::DeleteObjectVersion => {
                "s3:ReplicateDelete/s3:DeleteObject permissions missing for replication user".to_string()
            }
            ReplicationCheckFailureContext::BucketCheck => "target bucket check failed: access denied".to_string(),
            ReplicationCheckFailureContext::VersioningCheck => "target bucket versioning check failed: access denied".to_string(),
            ReplicationCheckFailureContext::ObjectLockCheck => "target object lock check failed: access denied".to_string(),
        };
    }

    let context = match context {
        ReplicationCheckFailureContext::BucketCheck => "target bucket check failed",
        ReplicationCheckFailureContext::VersioningCheck => "target bucket versioning check failed",
        ReplicationCheckFailureContext::ReplicateObject => "target replicate object check failed",
        ReplicationCheckFailureContext::ReplicateDeleteMarker => "target replicate delete-marker check failed",
        ReplicationCheckFailureContext::DeleteObjectVersion => "target delete object version check failed",
        ReplicationCheckFailureContext::ObjectLockCheck => "target object lock check failed",
    };

    // Remote messages and transport errors may echo a signed URL, credentials, or
    // endpoint user-info. Only expose the structured S3 code, which is sufficient
    // for operators and safe to return to an untrusted caller.
    match err.code.as_deref() {
        Some("NoSuchBucket" | "NotFound") => format!("{context}: target bucket does not exist"),
        Some(code) if code.chars().all(|ch| ch.is_ascii_alphanumeric()) => {
            bound_replication_check_error(format!("{context}: {code}"))
        }
        _ => format!("{context}: remote request failed"),
    }
}

fn is_object_lock_not_enabled_error(err: &S3ClientError) -> bool {
    matches!(
        err.code.as_deref(),
        Some("ObjectLockConfigurationNotFoundError" | "ObjectLockConfigurationNotFound")
    ) || err.message.as_deref().is_some_and(|message| {
        message.contains("Object Lock configuration does not exist")
            || message.contains("Object Lock is not enabled for this bucket")
    })
}

fn validate_replication_check_config_targets(
    targets: &BucketTargets,
    config: &PersistedReplicationConfiguration,
) -> S3Result<()> {
    let configured_arns = targets
        .targets
        .iter()
        .filter(|target| target.target_type == BucketTargetType::ReplicationService)
        .map(|target| target.arn.as_str())
        .collect::<HashSet<_>>();

    let role = config.role.trim();
    if !role.is_empty() {
        if !configured_arns.contains(role) {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                format!("replication config has stale target {role}"),
            ));
        }
        return Ok(());
    }

    for rule in &config.rules {
        if rule.status == Status::DISABLED.as_str() {
            continue;
        }

        let configured_arn = rule.destination.bucket.trim();
        if !configured_arn.is_empty() && !configured_arns.contains(configured_arn) {
            let rule_id = rule.id.as_deref().unwrap_or("<unknown>");
            return Err(s3_error!(
                InvalidRequest,
                "replication rule {rule_id} references stale target {configured_arn}"
            ));
        }
    }

    Ok(())
}

fn filter_replication_check_targets(targets: BucketTargets, config: &PersistedReplicationConfiguration) -> Vec<BucketTarget> {
    let referenced_arns = config
        .filter_all_replication_target_arns()
        .into_iter()
        .collect::<HashSet<_>>();

    targets
        .targets
        .into_iter()
        .filter(|target| target.target_type == BucketTargetType::ReplicationService)
        .filter(|target| referenced_arns.is_empty() || referenced_arns.contains(&target.arn))
        .collect()
}

async fn check_replication_target(
    bucket: &str,
    target: &BucketTarget,
    source_requires_object_lock: bool,
) -> ReplicationCheckTargetStatus {
    let mut result = ReplicationCheckTargetStatus {
        arn: target.arn.clone(),
        bucket: target.target_bucket.clone(),
        status: "OK".to_string(),
        error: None,
        phases: ReplicationCheckPhases::default(),
    };

    if target.target_bucket == bucket
        && !target.deployment_id.is_empty()
        && current_deployment_id().as_deref() == Some(target.deployment_id.as_str())
    {
        fail_replication_check_target(&mut result, "target bucket must not match source bucket on the same deployment");
        return result;
    }

    let target_client = match resolve_replication_target_client(bucket, target).await {
        Ok(client) => client,
        Err(_) => {
            fail_replication_check_target(&mut result, "target client initialization failed");
            return result;
        }
    };

    match target_client.client.head_bucket().bucket(&target.target_bucket).send().await {
        Ok(_) => result.phases.bucket = ReplicationCheckPhaseStatus::passed(),
        Err(err) => {
            let err = S3ClientError::from(err);
            let error = format_replication_check_client_error(&err, ReplicationCheckFailureContext::BucketCheck);
            result.phases.bucket = ReplicationCheckPhaseStatus::failed(&error);
            fail_replication_check_target(&mut result, error);
            return result;
        }
    }

    match target_client.get_bucket_versioning(&target.target_bucket).await {
        Ok(Some(_)) => result.phases.versioning = ReplicationCheckPhaseStatus::passed(),
        Ok(None) => {
            let error = "target bucket is not versioned";
            result.phases.versioning = ReplicationCheckPhaseStatus::failed(error);
            fail_replication_check_target(&mut result, error);
            return result;
        }
        Err(err) => {
            let error = format_replication_check_client_error(&err, ReplicationCheckFailureContext::VersioningCheck);
            result.phases.versioning = ReplicationCheckPhaseStatus::failed(&error);
            fail_replication_check_target(&mut result, error);
            return result;
        }
    }

    if source_requires_object_lock {
        match target_client_object_lock_enabled_with_client(&target_client, &target.target_bucket).await {
            Ok(true) => result.phases.object_lock = ReplicationCheckPhaseStatus::passed(),
            Ok(false) => {
                let error = "target bucket is not object lock enabled";
                result.phases.object_lock = ReplicationCheckPhaseStatus::failed(error);
                fail_replication_check_target(&mut result, error);
                return result;
            }
            Err(err) => {
                let error = format_replication_check_client_error(&err, ReplicationCheckFailureContext::ObjectLockCheck);
                result.phases.object_lock = ReplicationCheckPhaseStatus::failed(&error);
                fail_replication_check_target(&mut result, error);
                return result;
            }
        }
    } else {
        result.phases.object_lock = ReplicationCheckPhaseStatus::passed();
    }

    let probe_key = match allocate_replication_probe_key(&target_client, &target.target_bucket).await {
        Ok(key) => key,
        Err(error) => {
            result.phases.put = ReplicationCheckPhaseStatus::failed(&error);
            fail_replication_check_target(&mut result, error);
            return result;
        }
    };
    let mut operations = RemoteReplicationProbeOperations {
        client: &target_client,
        bucket: &target.target_bucket,
        key: &probe_key,
        time: OffsetDateTime::now_utc(),
    };
    execute_replication_probe(&mut result, &mut operations).await;

    // Sync the probe verdict into the runtime capability cache: the
    // replication worker then fails SSE-C replication closed on a flagged
    // target (or skips its own HEAD-back audit on a proven one) without
    // re-learning what the probe just established.
    match (result.phases.ssec_passthrough.status, result.phases.ssec_passthrough.code) {
        ("OK", _) => {
            BucketTargetSys::get()
                .record_ssec_passthrough_capability(&target.arn, SsecPassthroughCapability::Supported)
                .await;
        }
        ("FAILED", Some(REPLICATION_CHECK_CODE_SSEC_PASSTHROUGH)) => {
            BucketTargetSys::get()
                .record_ssec_passthrough_capability(&target.arn, SsecPassthroughCapability::Unsupported)
                .await;
        }
        _ => {}
    }
    // Same for the identity verdict: once a target is known to mint its own
    // version ids, the worker locates replicas by content identity instead of
    // re-driving PUTs whenever a version-addressed HEAD answers 404.
    match (result.phases.version_fidelity.status, result.phases.version_fidelity.code) {
        ("OK", _) => {
            BucketTargetSys::get().record_version_identity_capability(&target.arn, VersionIdentityCapability::Adopts);
        }
        ("FAILED", Some(REPLICATION_CHECK_CODE_VERSION_MISMATCH)) => {
            BucketTargetSys::get().record_version_identity_capability(&target.arn, VersionIdentityCapability::MintsOwn);
        }
        _ => {}
    }

    result
}

fn fail_replication_check_target(result: &mut ReplicationCheckTargetStatus, error: impl Into<String>) {
    result.status = "FAILED".to_string();
    if result.error.is_none() {
        result.error = Some(bound_replication_check_error(error.into()));
    }
}

async fn source_bucket_requires_object_lock(bucket: &str) -> S3Result<bool> {
    match metadata_sys::get_object_lock_config(bucket).await {
        Ok((config, _)) => Ok(config.object_lock_enabled.as_ref().is_some_and(|state| state == "Enabled")),
        Err(StorageError::ConfigNotFound) => Ok(false),
        Err(err) => Err(ApiError::from(err).into()),
    }
}

pub(super) async fn run_replication_check(bucket: &str) -> S3Result<AdminResponse> {
    if !BucketVersioningSys::enabled(bucket).await {
        return Err(s3_error!(
            InvalidRequest,
            "replication validation requires bucket versioning to be enabled"
        ));
    }

    let source_requires_object_lock = source_bucket_requires_object_lock(bucket).await?;
    let (config, _) = metadata_sys::get_replication_config(bucket).await.map_err(ApiError::from)?;
    let targets = metadata_sys::list_bucket_targets(bucket).await.map_err(ApiError::from)?;
    validate_replication_check_config_targets(&targets, &config)?;
    let replication_targets = filter_replication_check_targets(targets, &config);

    if replication_targets.is_empty() {
        return Err(s3_error!(
            InvalidRequest,
            "replication check requires at least one configured replication target"
        ));
    }

    let mut statuses = Vec::with_capacity(replication_targets.len());
    for target in &replication_targets {
        statuses.push(check_replication_target(bucket, target, source_requires_object_lock).await);
    }

    build_replication_check_response(statuses)
}

async fn target_client_object_lock_enabled_with_client(
    target_client: &TargetClient,
    target_bucket: &str,
) -> Result<bool, S3ClientError> {
    match target_client
        .client
        .get_object_lock_configuration()
        .bucket(target_bucket)
        .send()
        .await
    {
        Ok(res) => Ok(res
            .object_lock_configuration()
            .and_then(|cfg| cfg.object_lock_enabled())
            .is_some_and(|state| state.as_str() == "Enabled")),
        Err(err) => {
            let err = S3ClientError::from(err);
            if is_object_lock_not_enabled_error(&err) {
                Ok(false)
            } else {
                Err(err)
            }
        }
    }
}
