// Copyright 2026 RustFS Team
// SPDX-License-Identifier: Apache-2.0

//! Restriction registration only. These endpoints do not vend credentials.
use crate::admin::operation::{AdminRequest, AdminResponse, Operation};
use crate::admin::{
    auth::{AdminResourceScope, validate_admin_action_with_bucket_object_for_iam},
    plugin_runner::{self, ObjectReference, SubmitRequest},
    router::{AdminOperation, S3Router},
    runtime_sources,
    storage_api::s3::{self, S3Error, S3ErrorCode, S3Result},
};
use crate::{
    auth::{check_key_valid_with_context, get_session_token},
    server::ADMIN_PREFIX,
};
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use matchit::Params;
use rustfs_iam::delegation::{DelegationError, Restrictions};
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_s3_types::Body;
use serde::Deserialize;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub fn register_plugin_delegation_route(router: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    router.insert(Method::POST, &format!("{ADMIN_PREFIX}/v3/plugin-delegations"), AdminOperation(&Create))?;
    router.insert(Method::GET, &format!("{ADMIN_PREFIX}/v3/plugin-delegations/{{id}}"), AdminOperation(&Get))?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-delegations/{{id}}/revoke"),
        AdminOperation(&Revoke),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-delegations/{{id}}/submit"),
        AdminOperation(&Submit),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-delegations/{{id}}/runs/query"),
        AdminOperation(&QueryRun),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-delegations/{{id}}/runs/history"),
        AdminOperation(&HistoryRun),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-delegations/{{id}}/runs/cancel"),
        AdminOperation(&CancelRun),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-delegations/{{id}}/runs/page"),
        AdminOperation(&PageRuns),
    )?;
    router.insert(
        Method::POST,
        &format!("{ADMIN_PREFIX}/v3/plugin-delegations/{{id}}/runs/replay"),
        AdminOperation(&ReplayRun),
    )?;
    Ok(())
}

pub(super) fn storage_error(error: DelegationError) -> S3Error {
    match error {
        DelegationError::Invalid => s3::error(S3ErrorCode::InvalidRequest, "invalid delegation restriction"),
        DelegationError::NotFound | DelegationError::OwnerRejected => s3::error(S3ErrorCode::NoSuchKey, "delegation not found"),
        DelegationError::Unavailable => s3::error(S3ErrorCode::InternalError, "delegation unavailable"),
        _ => s3::error(S3ErrorCode::AccessDenied, "delegation rejected"),
    }
}

pub(super) async fn owner(
    req: &AdminRequest,
    action: AdminAction,
) -> S3Result<(std::sync::Arc<rustfs_iam::sys::IamSys<rustfs_iam::store::object::ObjectStore>>, String)> {
    let signed = req
        .credentials()
        .ok_or_else(|| s3::error(S3ErrorCode::AccessDenied, "authentication required"))?;
    if get_session_token(req.uri(), req.headers()).is_some() {
        return Err(s3::error(S3ErrorCode::AccessDenied, "original user required"));
    }
    let context = runtime_sources::app_context_from_envelope(req.envelope())
        .ok_or_else(|| s3::error(S3ErrorCode::InternalError, "application context unavailable"))?;
    let (credentials, root) = check_key_valid_with_context("", signed.access_key(), Some(context.as_ref())).await?;
    if root || credentials.is_temp() || credentials.is_service_account() {
        return Err(s3::error(S3ErrorCode::AccessDenied, "original user required"));
    }
    if !context.iam().is_ready() {
        return Err(s3::error(S3ErrorCode::InternalError, "iam unavailable"));
    }
    let iam = context.iam().handle();
    validate_admin_action_with_bucket_object_for_iam(
        iam.clone(),
        req.headers(),
        &credentials,
        false,
        Action::AdminAction(action),
        req.remote_addr(),
        AdminResourceScope::bucket_object("", ""),
    )
    .await?;
    Ok((iam, credentials.access_key))
}

pub(super) fn response<T: serde::Serialize>(value: &T) -> S3Result<AdminResponse> {
    let bytes =
        serde_json::to_vec(value).map_err(|_| s3::error(S3ErrorCode::InternalError, "delegation response unavailable"))?;
    let mut headers = HeaderMap::new();
    headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    headers.insert(http::header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    Ok(AdminResponse::with_headers(StatusCode::OK, Body::from(bytes), headers))
}

struct Create;
#[async_trait::async_trait]
impl Operation for Create {
    async fn call(&self, mut req: AdminRequest, _params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let (iam, owner) = owner(&req, AdminAction::CreatePluginDelegationAction).await?;
        let bytes = tokio::time::timeout(Duration::from_secs(10), req.take_body().store_all_limited(16384))
            .await
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "delegation body unavailable"))?
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "delegation body unavailable"))?;
        let restriction: Restrictions = serde_json::from_slice(&bytes)
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid delegation restriction"))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| s3::error(S3ErrorCode::InternalError, "clock unavailable"))?
            .as_secs();
        let record = iam
            .create_plugin_delegation(&owner, restriction, now)
            .await
            .map_err(storage_error)?;
        response(&record)
    }
}

struct Get;
#[async_trait::async_trait]
impl Operation for Get {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let (iam, owner) = owner(&req, AdminAction::GetPluginDelegationAction).await?;
        let id = params
            .get("id")
            .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "delegation id required"))?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| s3::error(S3ErrorCode::InternalError, "clock unavailable"))?
            .as_secs();
        let record = iam
            .read_active_plugin_delegation(&owner, id, now)
            .await
            .map_err(storage_error)?;
        response(&record)
    }
}

struct Revoke;
#[async_trait::async_trait]
impl Operation for Revoke {
    async fn call(&self, req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let (iam, owner) = owner(&req, AdminAction::RevokePluginDelegationAction).await?;
        let id = params
            .get("id")
            .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "delegation id required"))?;
        iam.revoke_plugin_delegation(&owner, id).await.map_err(storage_error)?;
        response(&serde_json::json!({"revoked":true,"id":id}))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitInput {
    version: u32,
    instance_id: String,
    idempotency_key: String,
    object: SubmitObject,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitObject {
    bucket: String,
    key: String,
    version_id: String,
}

struct Submit;
#[async_trait::async_trait]
impl Operation for Submit {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let (iam, signer) = owner(&req, AdminAction::SubmitPluginJobAction).await?;
        let id = params
            .get("id")
            .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "delegation id required"))?;
        let bytes = tokio::time::timeout(Duration::from_secs(10), req.take_body().store_all_limited(16384))
            .await
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "submit body unavailable"))?
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "submit body unavailable"))?;
        let input: SubmitInput =
            serde_json::from_slice(&bytes).map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid submit request"))?;
        if input.version != 1
            || !plugin_runner::literal(&input.instance_id)
            || !plugin_runner::valid_idempotency_key(&input.idempotency_key)
        {
            return Err(s3::error(S3ErrorCode::InvalidRequest, "invalid submit request"));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| s3::error(S3ErrorCode::InternalError, "clock unavailable"))?
            .as_secs();
        let record = iam
            .read_active_plugin_delegation(&signer, id, now)
            .await
            .map_err(storage_error)?;
        let deployment = plugin_runner::configured_deployment()?;
        if !plugin_runner::validate_delegation_scope(
            &record,
            &signer,
            id,
            &deployment,
            ObjectReference {
                bucket: &input.object.bucket,
                key: &input.object.key,
                version_id: &input.object.version_id,
            },
            now,
        ) {
            return Err(s3::error(S3ErrorCode::AccessDenied, "submit restriction rejected"));
        }
        let receipt = plugin_runner::submit(&SubmitRequest {
            version: input.version,
            instance_id: &input.instance_id,
            idempotency_key: &input.idempotency_key,
            object: ObjectReference {
                bucket: &input.object.bucket,
                key: &input.object.key,
                version_id: &input.object.version_id,
            },
            delegation_id: &record.id,
            delegation_revision: &record.revision,
            actual_signer_access_key: &signer,
        })
        .await?;
        response(&receipt)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct QueryRunInput {
    version: u32,
    run_id: uuid::Uuid,
}

struct QueryRun;
#[async_trait::async_trait]
impl Operation for QueryRun {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let (iam, signer) = owner(&req, AdminAction::GetPluginRunAction).await?;
        let id = params
            .get("id")
            .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "delegation id required"))?;
        let bytes = tokio::time::timeout(Duration::from_secs(10), req.take_body().store_all_limited(16384))
            .await
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "query body unavailable"))?
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "query body unavailable"))?;
        let input: QueryRunInput =
            serde_json::from_slice(&bytes).map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid query request"))?;
        if input.version != 1 || input.run_id.is_nil() {
            return Err(s3::error(S3ErrorCode::InvalidRequest, "invalid query request"));
        }
        let record = iam.read_owned_plugin_delegation(&signer, id).await.map_err(storage_error)?;
        let result = plugin_runner::query(&plugin_runner::QueryRequest {
            version: input.version,
            run_id: input.run_id,
            delegation_id: &record.id,
            delegation_revision: &record.revision,
            actual_signer_access_key: &signer,
        })
        .await?;
        response(&result)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryRunInput {
    version: u32,
    run_id: uuid::Uuid,
}

fn valid_history_run_input(input: &HistoryRunInput) -> bool {
    input.version == 1 && !input.run_id.is_nil()
}

struct HistoryRun;
#[async_trait::async_trait]
impl Operation for HistoryRun {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let (iam, signer) = owner(&req, AdminAction::ListPluginRunsAction).await?;
        let id = params
            .get("id")
            .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "delegation id required"))?;
        let bytes = tokio::time::timeout(Duration::from_secs(10), req.take_body().store_all_limited(16384))
            .await
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "history body unavailable"))?
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "history body unavailable"))?;
        let input: HistoryRunInput =
            serde_json::from_slice(&bytes).map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid history request"))?;
        if !valid_history_run_input(&input) {
            return Err(s3::error(S3ErrorCode::InvalidRequest, "invalid history request"));
        }
        let record = iam.read_owned_plugin_delegation(&signer, id).await.map_err(storage_error)?;
        let result = plugin_runner::history(&plugin_runner::HistoryRequest {
            version: input.version,
            run_id: input.run_id,
            delegation_id: &record.id,
            delegation_revision: &record.revision,
            actual_signer_access_key: &signer,
        })
        .await?;
        response(&result)
    }
}

struct CancelRun;
#[async_trait::async_trait]
impl Operation for CancelRun {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let (iam, signer) = owner(&req, AdminAction::CancelPluginRunAction).await?;
        let id = params
            .get("id")
            .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "delegation id required"))?;
        let bytes = tokio::time::timeout(Duration::from_secs(10), req.take_body().store_all_limited(16384))
            .await
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "cancel body unavailable"))?
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "cancel body unavailable"))?;
        let input: QueryRunInput =
            serde_json::from_slice(&bytes).map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid cancel request"))?;
        if input.version != 1 || input.run_id.is_nil() {
            return Err(s3::error(S3ErrorCode::InvalidRequest, "invalid cancel request"));
        }
        let record = iam.read_owned_plugin_delegation(&signer, id).await.map_err(storage_error)?;
        let result = plugin_runner::cancel(&plugin_runner::CancelRequest {
            version: input.version,
            run_id: input.run_id,
            delegation_id: &record.id,
            delegation_revision: &record.revision,
            actual_signer_access_key: &signer,
        })
        .await?;
        response(&result)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageRunsInput {
    version: u32,
    limit: u32,
    #[serde(default)]
    cursor: Option<plugin_runner::RunPageCursor>,
}

fn valid_page_runs_input(input: &PageRunsInput) -> bool {
    input.version == 1 && (1..=100).contains(&input.limit) && !input.cursor.is_some_and(|cursor| cursor.run_id.is_nil())
}

struct PageRuns;
#[async_trait::async_trait]
impl Operation for PageRuns {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let (iam, signer) = owner(&req, AdminAction::ListPluginRunsAction).await?;
        let id = params
            .get("id")
            .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "delegation id required"))?;
        let bytes = tokio::time::timeout(Duration::from_secs(10), req.take_body().store_all_limited(16384))
            .await
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "page body unavailable"))?
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "page body unavailable"))?;
        let input: PageRunsInput =
            serde_json::from_slice(&bytes).map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid page request"))?;
        if !valid_page_runs_input(&input) {
            return Err(s3::error(S3ErrorCode::InvalidRequest, "invalid page request"));
        }
        let record = iam.read_owned_plugin_delegation(&signer, id).await.map_err(storage_error)?;
        let result = plugin_runner::page(&plugin_runner::PageRequest {
            version: input.version,
            actual_signer_access_key: &signer,
            delegation_id: &record.id,
            delegation_revision: &record.revision,
            limit: input.limit,
            cursor: input.cursor,
        })
        .await?;
        response(&result)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayRunInput {
    version: u32,
    origin_run_id: uuid::Uuid,
    idempotency_key: String,
    deadline_unix_micros: i64,
}

fn valid_replay_run_input(input: &ReplayRunInput) -> bool {
    input.version == 1
        && !input.origin_run_id.is_nil()
        && plugin_runner::valid_idempotency_key(&input.idempotency_key)
        && input.deadline_unix_micros > 0
}

struct ReplayRun;
#[async_trait::async_trait]
impl Operation for ReplayRun {
    async fn call(&self, mut req: AdminRequest, params: Params<'_, '_>) -> S3Result<AdminResponse> {
        let (iam, signer) = owner(&req, AdminAction::ReplayPluginRunAction).await?;
        let id = params
            .get("id")
            .ok_or_else(|| s3::error(S3ErrorCode::InvalidRequest, "delegation id required"))?;
        let bytes = tokio::time::timeout(Duration::from_secs(10), req.take_body().store_all_limited(16384))
            .await
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "replay body unavailable"))?
            .map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "replay body unavailable"))?;
        let input: ReplayRunInput =
            serde_json::from_slice(&bytes).map_err(|_| s3::error(S3ErrorCode::InvalidRequest, "invalid replay request"))?;
        if !valid_replay_run_input(&input) {
            return Err(s3::error(S3ErrorCode::InvalidRequest, "invalid replay request"));
        }
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| s3::error(S3ErrorCode::InternalError, "clock unavailable"))?
            .as_secs();
        let record = iam
            .read_active_plugin_delegation(&signer, id, now)
            .await
            .map_err(storage_error)?;
        let result = plugin_runner::replay(&plugin_runner::ReplayRequest {
            version: input.version,
            origin_run_id: input.origin_run_id,
            idempotency_key: &input.idempotency_key,
            deadline_unix_micros: input.deadline_unix_micros,
            actual_signer_access_key: &signer,
            delegation_id: &record.id,
            delegation_revision: &record.revision,
        })
        .await?;
        response(&result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn delegation_mutations_require_authenticated_original_user() {
        for operation in 0..8 {
            let req =
                crate::admin::operation::test_support::TestRequest::new(Method::POST, "/rustfs/admin/v3/plugin-delegations")
                    .headers(HeaderMap::new())
                    .extensions(http::Extensions::new())
                    .body(Body::from("{}".to_owned()))
                    .build();
            let result = if operation == 0 {
                Get.call(req, Params::new()).await
            } else if operation == 1 {
                Revoke.call(req, Params::new()).await
            } else if operation == 2 {
                Submit.call(req, Params::new()).await
            } else if operation == 3 {
                QueryRun.call(req, Params::new()).await
            } else if operation == 4 {
                HistoryRun.call(req, Params::new()).await
            } else if operation == 5 {
                CancelRun.call(req, Params::new()).await
            } else if operation == 6 {
                PageRuns.call(req, Params::new()).await
            } else if operation == 7 {
                ReplayRun.call(req, Params::new()).await
            } else {
                Create.call(req, Params::new()).await
            };
            let error = result.expect_err("unsigned mutations must not reach persistence");
            assert_eq!(error.code(), &S3ErrorCode::AccessDenied);
        }
    }

    #[test]
    fn submit_dto_rejects_client_authority_fields_and_null_versions() {
        assert!(serde_json::from_str::<SubmitInput>(
            r#"{"version":1,"instance_id":"img","idempotency_key":"k","object":{"bucket":"b","key":"k","version_id":"v"},"actual_signer_access_key":"forged"}"#
        )
        .is_err());
        let input: SubmitInput = serde_json::from_str(
            r#"{"version":1,"instance_id":"img","idempotency_key":"k","object":{"bucket":"b","key":"a/x","version_id":"null"}}"#,
        )
        .unwrap();
        let record = rustfs_iam::delegation::Delegation {
            version: 1,
            id: "a".repeat(32),
            revision: "a".repeat(32),
            owner_access_key: "owner".into(),
            created_at_unix_seconds: 1,
            restrictions: Restrictions {
                version: None,
                output_bucket: None,
                deployment: "prod".into(),
                bucket: "b".into(),
                input_prefix: "a/".into(),
                output_prefix: "out/".into(),
                expires_at_unix_seconds: 100,
            },
        };
        assert!(!plugin_runner::validate_delegation_scope(
            &record,
            "owner",
            &record.id,
            "prod",
            ObjectReference {
                bucket: &input.object.bucket,
                key: &input.object.key,
                version_id: &input.object.version_id,
            },
            2,
        ));
    }

    #[test]
    fn query_run_dto_rejects_client_authority_fields() {
        let run_id = uuid::Uuid::from_u128(1);
        let input: QueryRunInput =
            serde_json::from_str(&format!(r#"{{"version":1,"run_id":"{run_id}"}}"#)).expect("valid run query");
        assert_eq!(input.run_id, run_id);
        assert!(
            serde_json::from_str::<QueryRunInput>(&format!(
                r#"{{"version":1,"run_id":"{run_id}","actual_signer_access_key":"forged"}}"#
            ))
            .is_err()
        );
    }

    #[test]
    fn history_run_dto_is_strict_and_rejects_nil_run() {
        let run_id = uuid::Uuid::from_u128(1);
        let input: HistoryRunInput =
            serde_json::from_str(&format!(r#"{{"version":1,"run_id":"{run_id}"}}"#)).expect("valid history request");
        assert!(valid_history_run_input(&input));
        for raw in [
            r#"{"version":1,"run_id":"00000000-0000-0000-0000-000000000000"}"#,
            r#"{"version":2,"run_id":"00000000-0000-0000-0000-000000000001"}"#,
            r#"{"version":1,"run_id":"00000000-0000-0000-0000-000000000001","actual_signer_access_key":"forged"}"#,
        ] {
            let rejected = match serde_json::from_str::<HistoryRunInput>(raw) {
                Ok(input) => !valid_history_run_input(&input),
                Err(_) => true,
            };
            assert!(rejected, "accepted invalid history request: {raw}");
        }
    }

    #[test]
    fn page_runs_dto_is_strict_and_bounded() {
        let cursor = plugin_runner::RunPageCursor {
            created_at_unix_micros: 10,
            run_id: uuid::Uuid::from_u128(1),
        };
        let input: PageRunsInput = serde_json::from_str(
            r#"{"version":1,"limit":100,"cursor":{"created_at_unix_micros":10,"run_id":"00000000-0000-0000-0000-000000000001"}}"#,
        )
        .expect("valid page request");
        assert_eq!(input.cursor, Some(cursor));
        for raw in [
            r#"{"version":1,"limit":0}"#,
            r#"{"version":1,"limit":101}"#,
            r#"{"version":1,"limit":1,"unknown":true}"#,
            r#"{"version":1,"limit":1,"actual_signer_access_key":"forged"}"#,
            r#"{"version":1,"limit":1,"cursor":{"created_at_unix_micros":10,"run_id":"00000000-0000-0000-0000-000000000000"}}"#,
        ] {
            let rejected = match serde_json::from_str::<PageRunsInput>(raw) {
                Ok(input) => !valid_page_runs_input(&input),
                Err(_) => true,
            };
            assert!(rejected, "accepted invalid page request: {raw}");
        }
    }

    #[test]
    fn replay_run_dto_is_strict_and_bounded() {
        let run_id = uuid::Uuid::from_u128(1);
        let input: ReplayRunInput = serde_json::from_str(&format!(
            r#"{{"version":1,"origin_run_id":"{run_id}","idempotency_key":"retry-1","deadline_unix_micros":123}}"#
        ))
        .expect("valid replay request");
        assert!(valid_replay_run_input(&input));
        for raw in [
            r#"{"version":1,"origin_run_id":"00000000-0000-0000-0000-000000000000","idempotency_key":"k","deadline_unix_micros":123}"#.to_string(),
            format!(r#"{{"version":1,"origin_run_id":"{run_id}","idempotency_key":"","deadline_unix_micros":123}}"#),
            format!(r#"{{"version":1,"origin_run_id":"{run_id}","idempotency_key":"k","deadline_unix_micros":0}}"#),
            format!(
                r#"{{"version":1,"origin_run_id":"{run_id}","idempotency_key":"k","deadline_unix_micros":123,"actual_signer_access_key":"forged"}}"#
            ),
            format!(
                r#"{{"version":1,"origin_run_id":"{run_id}","idempotency_key":"{}","deadline_unix_micros":123}}"#,
                "x".repeat(1025)
            ),
        ] {
            let rejected = match serde_json::from_str::<ReplayRunInput>(&raw) {
                Ok(input) => !valid_replay_run_input(&input),
                Err(_) => true,
            };
            assert!(rejected, "accepted invalid replay request: {raw}");
        }
    }
}
