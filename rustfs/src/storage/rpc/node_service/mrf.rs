// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

//! Authenticated MRF node control. Admin permission is checked before forwarding;
//! the signed peer body preserves that actor and the exact target topology.

use rustfs_heal::heal::mrf_queue::management::{self, MrfLifecycleSummary, MrfResponsibilityObservation};
use rustfs_heal::heal::mrf_queue::{self, MrfLegacyResponsibilitySnapshot, MrfLifecycleControlError};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct MrfLegacyResponsibilityActionRequest {
    pub(crate) action: MrfLegacyResponsibilityAction,
    pub(crate) responsibility_id: uuid::Uuid,
    pub(crate) expected_bucket_incarnation_id: Option<uuid::Uuid>,
    #[serde(default)]
    pub(crate) acknowledge_unverified_data_risk: Option<bool>,
    #[serde(default)]
    pub(crate) acknowledge_unknown_source_incarnation: Option<bool>,
    #[serde(default)]
    pub(crate) acknowledge_bucket_incarnation_mismatch: Option<bool>,
    #[serde(default)]
    pub(crate) reason: Option<String>,
    #[serde(default)]
    pub(crate) reference: Option<String>,
    #[serde(default)]
    pub(crate) request_id: Option<uuid::Uuid>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum MrfLegacyResponsibilityAction {
    Refresh,
    Recheck,
    AcceptUnverifiedRisk,
}

pub(crate) fn validate_mrf_legacy_responsibility_action(
    action: &MrfLegacyResponsibilityActionRequest,
) -> Result<(), &'static str> {
    if action.responsibility_id.is_nil() || action.expected_bucket_incarnation_id.is_some_and(|id| id.is_nil()) {
        return Err("responsibility and bucket incarnation IDs must be non-nil");
    }
    if action.expected_bucket_incarnation_id.is_none() && !matches!(action.action, MrfLegacyResponsibilityAction::Refresh) {
        return Err("a known bucket incarnation is required for recheck and risk acceptance");
    }
    match action.action {
        MrfLegacyResponsibilityAction::Refresh | MrfLegacyResponsibilityAction::Recheck => {
            if action.acknowledge_unverified_data_risk.is_some()
                || action.acknowledge_unknown_source_incarnation.is_some()
                || action.acknowledge_bucket_incarnation_mismatch.is_some()
                || action.reason.is_some()
                || action.reference.is_some()
            {
                return Err("recheck does not accept risk-acknowledgment fields");
            }
        }
        MrfLegacyResponsibilityAction::AcceptUnverifiedRisk => {
            if action.acknowledge_unverified_data_risk != Some(true) {
                return Err("acknowledgeUnverifiedDataRisk must be true");
            }
            if action.acknowledge_unknown_source_incarnation.is_none() || action.acknowledge_bucket_incarnation_mismatch.is_none()
            {
                return Err("both source-incarnation acknowledgment fields are required");
            }
            if action
                .reason
                .as_deref()
                .is_none_or(|value| value.trim().is_empty() || value.len() > 1024)
            {
                return Err("reason is required and limited to 1024 UTF-8 bytes");
            }
            if action
                .reference
                .as_deref()
                .is_none_or(|value| value.trim().is_empty() || value.len() > 256)
            {
                return Err("reference is required and limited to 256 UTF-8 bytes");
            }
            if action.request_id.is_none_or(|request_id| request_id.is_nil()) {
                return Err("requestId must be non-nil");
            }
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(
    tag = "operation",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub(crate) enum MrfOperation {
    // A struct variant makes Serde enforce unknown-field rejection for this command.
    Summary {},
    List {
        cursor: Option<Uuid>,
        limit: usize,
    },
    Get {
        id: Uuid,
    },
    Action {
        actor: String,
        request: Box<MrfLegacyResponsibilityActionRequest>,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MrfActionResult {
    pub responsibility_id: Uuid,
    pub state: String,
    pub data_verified: bool,
    pub durable_responsibility_retained: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "result", rename_all = "camelCase", rename_all_fields = "camelCase")]
pub(crate) enum MrfReply {
    Summary {
        summary: MrfLifecycleSummary,
    },
    List {
        snapshot: MrfLegacyResponsibilitySnapshot,
        observed_at_ms: u64,
    },
    Get {
        observation: Box<MrfResponsibilityObservation>,
    },
    Action {
        action: MrfActionResult,
    },
    Error {
        code: String,
    },
}

impl MrfReply {
    pub fn is_success(&self) -> bool {
        !matches!(
            self,
            Self::Error { .. }
                | Self::Summary {
                    summary: MrfLifecycleSummary {
                        checkpoint_readable: false,
                        ..
                    }
                }
        )
    }
}

pub(crate) fn node_id(grid_host: &str) -> String {
    use sha2::{Digest, Sha256};
    hex_simd::encode_to_string(Sha256::digest(grid_host.as_bytes()), hex_simd::AsciiCase::Lower)
}

pub(crate) async fn execute(operation: MrfOperation) -> MrfReply {
    let result = match operation {
        MrfOperation::Summary {} => management::lifecycle_summary().map(|summary| MrfReply::Summary { summary }),
        MrfOperation::List { cursor, limit } => {
            mrf_queue::list_legacy_responsibilities(cursor, limit)
                .await
                .map(|snapshot| MrfReply::List {
                    snapshot,
                    observed_at_ms: now_ms(),
                })
        }
        MrfOperation::Get { id } => management::responsibility(id).await.map(|observation| MrfReply::Get {
            observation: Box::new(observation),
        }),
        MrfOperation::Action { actor, request } => execute_action(*request, actor)
            .await
            .map(|action| MrfReply::Action { action }),
    };
    result.unwrap_or_else(|error| MrfReply::Error {
        code: error_code(&error).into(),
    })
}

pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |time| u64::try_from(time.as_millis()).unwrap_or(u64::MAX))
}

fn error_code(error: &MrfLifecycleControlError) -> &'static str {
    match error {
        MrfLifecycleControlError::Busy => "busy",
        MrfLifecycleControlError::Unavailable => "unavailable",
        MrfLifecycleControlError::CheckpointUnreadable => "checkpoint_unreadable",
        MrfLifecycleControlError::StaleGeneration => "stale_generation",
        MrfLifecycleControlError::NotHeld => "not_held",
        MrfLifecycleControlError::IncarnationChanged => "incarnation_changed",
        MrfLifecycleControlError::BucketIdentityUnresolved => "identity_unresolved",
        MrfLifecycleControlError::BucketIdentityUnavailable(_) => "identity_unavailable",
        MrfLifecycleControlError::InvalidAction(_) => "invalid_action",
        MrfLifecycleControlError::Persistence => "persistence_failed",
    }
}

const MAX_ACCEPTED_RISK_ACTOR_BYTES: usize = 256;

pub(crate) async fn execute_action(
    action: MrfLegacyResponsibilityActionRequest,
    actor: String,
) -> Result<MrfActionResult, MrfLifecycleControlError> {
    validate_mrf_legacy_responsibility_action(&action)
        .map_err(|reason| MrfLifecycleControlError::InvalidAction(reason.into()))?;
    if actor.trim().is_empty()
        || (matches!(action.action, MrfLegacyResponsibilityAction::AcceptUnverifiedRisk)
            && actor.len() > MAX_ACCEPTED_RISK_ACTOR_BYTES)
    {
        return Err(MrfLifecycleControlError::InvalidAction("invalid authenticated actor".into()));
    }
    let state = match action.action {
        MrfLegacyResponsibilityAction::Refresh => {
            mrf_queue::refresh_legacy_responsibility(action.responsibility_id, action.expected_bucket_incarnation_id).await?;
            "refreshed"
        }
        MrfLegacyResponsibilityAction::Recheck => {
            mrf_queue::recheck_legacy_responsibility(
                action.responsibility_id,
                action
                    .expected_bucket_incarnation_id
                    .ok_or_else(|| MrfLifecycleControlError::InvalidAction("missing bucket incarnation".into()))?,
            )
            .await?;
            "active"
        }
        MrfLegacyResponsibilityAction::AcceptUnverifiedRisk => {
            mrf_queue::accept_unverified_legacy_risk(mrf_queue::MrfLegacyRiskAcceptanceRequest {
                responsibility_id: action.responsibility_id,
                expected_bucket_incarnation_id: action
                    .expected_bucket_incarnation_id
                    .ok_or_else(|| MrfLifecycleControlError::InvalidAction("missing bucket incarnation".into()))?,
                acknowledge_unknown_source_incarnation: action.acknowledge_unknown_source_incarnation == Some(true),
                acknowledge_incarnation_mismatch: action.acknowledge_bucket_incarnation_mismatch == Some(true),
                actor,
                reason: action
                    .reason
                    .ok_or_else(|| MrfLifecycleControlError::InvalidAction("missing reason".into()))?,
                reference: action
                    .reference
                    .ok_or_else(|| MrfLifecycleControlError::InvalidAction("missing reference".into()))?,
                request_id: action
                    .request_id
                    .ok_or_else(|| MrfLifecycleControlError::InvalidAction("missing request ID".into()))?,
            })
            .await?;
            "operatorAcceptedUnverified"
        }
    };
    Ok(MrfActionResult {
        responsibility_id: action.responsibility_id,
        state: state.into(),
        data_verified: false,
        durable_responsibility_retained: true,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_ACCEPTED_RISK_ACTOR_BYTES, MrfLegacyResponsibilityAction, MrfLegacyResponsibilityActionRequest, execute_action,
    };
    use rustfs_heal::heal::mrf_queue::MrfLifecycleControlError;
    use uuid::Uuid;

    fn refresh_request() -> MrfLegacyResponsibilityActionRequest {
        MrfLegacyResponsibilityActionRequest {
            action: MrfLegacyResponsibilityAction::Refresh,
            responsibility_id: Uuid::from_u128(1),
            expected_bucket_incarnation_id: None,
            acknowledge_unverified_data_risk: None,
            acknowledge_unknown_source_incarnation: None,
            acknowledge_bucket_incarnation_mismatch: None,
            reason: None,
            reference: None,
            request_id: None,
        }
    }

    fn accept_risk_request() -> MrfLegacyResponsibilityActionRequest {
        MrfLegacyResponsibilityActionRequest {
            action: MrfLegacyResponsibilityAction::AcceptUnverifiedRisk,
            responsibility_id: Uuid::from_u128(1),
            expected_bucket_incarnation_id: Some(Uuid::from_u128(2)),
            acknowledge_unverified_data_risk: Some(true),
            acknowledge_unknown_source_incarnation: Some(true),
            acknowledge_bucket_incarnation_mismatch: Some(false),
            reason: Some("accepted legacy unverified risk".into()),
            reference: Some("INC-3194".into()),
            request_id: Some(Uuid::from_u128(3)),
        }
    }

    #[tokio::test]
    async fn refresh_accepts_authenticated_actor_above_256_bytes_while_risk_acceptance_caps_it() {
        let long_root_actor = "M".repeat(257);
        let refresh_err = execute_action(refresh_request(), long_root_actor.clone())
            .await
            .expect_err("uninitialized queue returns unavailable after passing actor validation");
        assert!(
            !matches!(&refresh_err, MrfLifecycleControlError::InvalidAction(reason) if reason == "invalid authenticated actor"),
            "refresh must not reject a 257-byte authenticated root actor: {refresh_err:?}"
        );

        let empty_err = execute_action(refresh_request(), "   ".into())
            .await
            .expect_err("blank actor must be rejected");
        assert!(matches!(empty_err, MrfLifecycleControlError::InvalidAction(reason) if reason == "invalid authenticated actor"));

        let risk_err = execute_action(accept_risk_request(), long_root_actor)
            .await
            .expect_err("persisted risk acceptance actor above 256 bytes must be rejected");
        assert!(matches!(risk_err, MrfLifecycleControlError::InvalidAction(reason) if reason == "invalid authenticated actor"));

        let bounded_actor = "M".repeat(MAX_ACCEPTED_RISK_ACTOR_BYTES);
        let bounded_err = execute_action(accept_risk_request(), bounded_actor)
            .await
            .expect_err("uninitialized queue returns unavailable after passing actor validation");
        assert!(
            !matches!(&bounded_err, MrfLifecycleControlError::InvalidAction(reason) if reason == "invalid authenticated actor"),
            "256-byte risk acceptance actor must pass validation: {bounded_err:?}"
        );
    }
}
