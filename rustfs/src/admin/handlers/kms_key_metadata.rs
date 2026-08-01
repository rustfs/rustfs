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

//! KMS key metadata admin API handlers: description updates and tagging.
//!
//! These endpoints change what a key is labelled with, never its material or
//! its state, which is why they are separate from the lifecycle endpoints in
//! [`super::kms_key_lifecycle`] and carry their own policy actions.

use super::kms_audit::{KmsAdminAudit, KmsAdminOperation};
use super::kms_keys::scoped_key_id;
use crate::admin::auth::validate_admin_request_with_kms_key;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::current_kms_runtime_service_manager;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use hyper::{HeaderMap, Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_kms::{
    KmsError, ObjectEncryptionService, TagKeyRequest, UntagKeyRequest, UpdateKeyDescriptionRequest,
    types::{DescribeKeyRequest, KeyMetadata},
};
use rustfs_policy::policy::action::{Action, KmsAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{error, info, warn};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_KMS_KEYS: &str = "kms_key_metadata";
const EVENT_ADMIN_KMS_KEYS_STATE: &str = "admin_kms_keys_state";

/// Replace a key's description. An empty description clears the stored value.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateKmsKeyDescriptionRequest {
    pub key_id: String,
    pub description: String,
}

/// Add or overwrite key tags. Tags outside `tags` are left untouched.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TagKmsKeyRequest {
    pub key_id: String,
    pub tags: HashMap<String, String>,
}

/// Remove key tags. Tag keys that are not set are ignored.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UntagKmsKeyRequest {
    pub key_id: String,
    pub tag_keys: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct KmsKeyMetadataResponse {
    pub success: bool,
    pub message: String,
    pub key_id: String,
    pub key_metadata: Option<KeyMetadata>,
}

/// One metadata mutation, already parsed from its endpoint's request body.
#[derive(Debug, Clone)]
enum MetadataUpdate {
    Description(String),
    SetTags(HashMap<String, String>),
    RemoveTags(Vec<String>),
}

impl MetadataUpdate {
    fn action(&self) -> &'static str {
        match self {
            Self::Description(_) => "update_key_description",
            Self::SetTags(_) => "tag_key",
            Self::RemoveTags(_) => "untag_key",
        }
    }

    fn verb(&self) -> &'static str {
        match self {
            Self::Description(_) => "update key description",
            Self::SetTags(_) => "tag key",
            Self::RemoveTags(_) => "untag key",
        }
    }

    fn past_tense(&self) -> &'static str {
        match self {
            Self::Description(_) => "key description updated",
            Self::SetTags(_) => "key tags updated",
            Self::RemoveTags(_) => "key tags removed",
        }
    }
}

fn kms_update_key_description_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::UpdateKeyDescriptionAction)]
}

fn kms_tag_key_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::TagResourceAction)]
}

fn kms_untag_key_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::UntagResourceAction)]
}

pub fn register_kms_key_metadata_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/update-description").as_str(),
        AdminOperation(&UpdateKmsKeyDescriptionHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/tag").as_str(),
        AdminOperation(&TagKmsKeyHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/untag").as_str(),
        AdminOperation(&UntagKmsKeyHandler {}),
    )?;

    Ok(())
}

/// Map a metadata update failure to an HTTP status.
///
/// `UnsupportedCapability` is a permanent gap of the configured backend (a
/// Static KMS deployment owns no mutable key record), not a missing resource:
/// it must surface as 501, never 404. Rewriting the reserved `name` tag is
/// rejected as `InvalidOperation` by the KMS layer and stays a 400, so a caller
/// cannot mistake it for a backend fault and retry it.
fn metadata_error_status(error: &KmsError) -> StatusCode {
    match error {
        KmsError::KeyNotFound { .. } => StatusCode::NOT_FOUND,
        KmsError::UnsupportedCapability { .. } => StatusCode::NOT_IMPLEMENTED,
        KmsError::InvalidOperation { .. } | KmsError::ValidationError { .. } => StatusCode::BAD_REQUEST,
        // Damaged or missing key material is an integrity fault of an existing
        // key: it must surface as a server error, never as NOT_FOUND (the key
        // exists) and never as a retryable backend outage.
        KmsError::MaterialMissing { .. }
        | KmsError::MaterialCorrupt { .. }
        | KmsError::MaterialAuthenticationFailed { .. }
        | KmsError::UnsupportedFormatVersion { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Apply one metadata update and build the HTTP reply.
///
/// Split from the handlers so the status/response contract is unit-testable
/// without the admin auth stack.
async fn execute_metadata_update(
    service: &ObjectEncryptionService,
    key_id: &str,
    update: MetadataUpdate,
) -> (StatusCode, KmsKeyMetadataResponse, Option<KmsError>) {
    let result = match &update {
        MetadataUpdate::Description(description) => service
            .update_key_description(UpdateKeyDescriptionRequest {
                key_id: key_id.to_string(),
                description: description.clone(),
            })
            .await
            .map(|_| ()),
        MetadataUpdate::SetTags(tags) => service
            .tag_key(TagKeyRequest {
                key_id: key_id.to_string(),
                tags: tags.clone(),
            })
            .await
            .map(|_| ()),
        MetadataUpdate::RemoveTags(tag_keys) => service
            .untag_key(UntagKeyRequest {
                key_id: key_id.to_string(),
                tag_keys: tag_keys.clone(),
            })
            .await
            .map(|_| ()),
    };

    match result {
        Ok(()) => {
            info!(
                event = EVENT_ADMIN_KMS_KEYS_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                action = update.action(),
                key_id = %key_id,
                state = "completed",
                "admin kms keys state"
            );
            // Best effort: the update already succeeded, so a failing describe
            // only drops the metadata echo from the response.
            let key_metadata = match service
                .describe_key(DescribeKeyRequest {
                    key_id: key_id.to_string(),
                })
                .await
            {
                Ok(response) => Some(response.key_metadata),
                Err(error) => {
                    warn!(
                        event = EVENT_ADMIN_KMS_KEYS_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                        action = update.action(),
                        key_id = %key_id,
                        error = %error,
                        "key metadata update succeeded but describe failed"
                    );
                    None
                }
            };
            (
                StatusCode::OK,
                KmsKeyMetadataResponse {
                    success: true,
                    message: update.past_tense().to_string(),
                    key_id: key_id.to_string(),
                    key_metadata,
                },
                None,
            )
        }
        Err(error) => {
            error!(
                event = EVENT_ADMIN_KMS_KEYS_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                action = update.action(),
                key_id = %key_id,
                result = "failed",
                error = %error,
                "admin kms keys state"
            );
            (
                metadata_error_status(&error),
                KmsKeyMetadataResponse {
                    success: false,
                    message: format!("Failed to {}: {error}", update.verb()),
                    key_id: key_id.to_string(),
                    key_metadata: None,
                },
                Some(error),
            )
        }
    }
}

fn json_response(status: StatusCode, response: &KmsKeyMetadataResponse) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, "application/json".parse().expect("static content type should parse"));
    Ok(S3Response::with_headers((status, Body::from(data)), headers))
}

fn unavailable_response(message: &str, key_id: String) -> S3Result<S3Response<(StatusCode, Body)>> {
    json_response(
        StatusCode::SERVICE_UNAVAILABLE,
        &KmsKeyMetadataResponse {
            success: false,
            message: message.to_string(),
            key_id,
            key_metadata: None,
        },
    )
}

/// Parse the request body of a metadata endpoint into the key it targets and
/// the update to apply.
type ParseUpdate = fn(&[u8]) -> S3Result<(String, MetadataUpdate)>;

fn parse_update_description(body: &[u8]) -> S3Result<(String, MetadataUpdate)> {
    let request: UpdateKmsKeyDescriptionRequest =
        serde_json::from_slice(body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?;
    Ok((request.key_id, MetadataUpdate::Description(request.description)))
}

fn parse_tag_key(body: &[u8]) -> S3Result<(String, MetadataUpdate)> {
    let request: TagKmsKeyRequest = serde_json::from_slice(body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?;
    Ok((request.key_id, MetadataUpdate::SetTags(request.tags)))
}

fn parse_untag_key(body: &[u8]) -> S3Result<(String, MetadataUpdate)> {
    let request: UntagKmsKeyRequest =
        serde_json::from_slice(body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?;
    Ok((request.key_id, MetadataUpdate::RemoveTags(request.tag_keys)))
}

async fn handle_metadata_request(
    mut req: S3Request<Body>,
    actions: Vec<Action>,
    audit_operation: KmsAdminOperation,
    parse: ParseUpdate,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let Some(cred) = req.credentials else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) = check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

    // The body names the key this operation acts on, so it has to be resolved
    // before the gate runs. A read failure is surfaced only afterwards so an
    // unauthorized caller still sees AccessDenied.
    let body = req.input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await;
    let scoped = body.as_ref().ok().and_then(|body| scoped_key_id(body, &req.uri));
    let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

    audit.gate_admin(
        validate_admin_request_with_kms_key(
            &req.headers,
            &cred,
            owner,
            false,
            actions,
            req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
            scoped.as_deref().unwrap_or_default(),
        )
        .await,
        audit_operation,
        scoped.as_deref(),
    )?;

    let body = body.map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

    if body.is_empty() {
        return Err(s3_error!(InvalidRequest, "a request body is required"));
    }

    let (key_id, update) = parse(&body)?;

    // An authorized attempt that never reached the KMS is still an attempt: audit
    // it before answering, or a caller could probe these endpoints while the
    // service is down and leave no trace.
    let Some(service_manager) = current_kms_runtime_service_manager() else {
        audit.finish_with_class(audit_operation, Some("service_unavailable"));
        return unavailable_response("kms service manager is not initialized", key_id);
    };

    let Some(service) = service_manager.get_encryption_service().await else {
        audit.finish_with_class(audit_operation, Some("service_unavailable"));
        return unavailable_response("kms service is not running", key_id);
    };

    let (status, response, error) = execute_metadata_update(&service, &key_id, update).await;
    audit.finish(audit_operation, Some(key_id.as_str()), error.as_ref());
    json_response(status, &response)
}

/// Replace a KMS key's description
pub struct UpdateKmsKeyDescriptionHandler {}

#[async_trait::async_trait]
impl Operation for UpdateKmsKeyDescriptionHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        handle_metadata_request(
            req,
            kms_update_key_description_actions(),
            KmsAdminOperation::UpdateKeyDescription,
            parse_update_description,
        )
        .await
    }
}

/// Add or overwrite KMS key tags
pub struct TagKmsKeyHandler {}

#[async_trait::async_trait]
impl Operation for TagKmsKeyHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        handle_metadata_request(req, kms_tag_key_actions(), KmsAdminOperation::TagResource, parse_tag_key).await
    }
}

/// Remove KMS key tags
pub struct UntagKmsKeyHandler {}

#[async_trait::async_trait]
impl Operation for UntagKmsKeyHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        handle_metadata_request(req, kms_untag_key_actions(), KmsAdminOperation::UntagResource, parse_untag_key).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use rustfs_kms::KmsManager;
    use rustfs_kms::backends::local::LocalKmsBackend;
    use rustfs_kms::backends::static_kms::StaticKmsBackend;
    use rustfs_kms::config::KmsConfig;
    use rustfs_kms::types::CreateKeyRequest;
    use rustfs_policy::policy::action::AdminAction;
    use std::sync::Arc;

    async fn local_service(temp_dir: &tempfile::TempDir) -> ObjectEncryptionService {
        let config = KmsConfig::local(temp_dir.path().to_path_buf()).with_insecure_development_defaults();
        let backend = Arc::new(
            LocalKmsBackend::new(config.clone())
                .await
                .expect("local backend should build"),
        );
        ObjectEncryptionService::new(KmsManager::new(backend, config))
    }

    /// Static KMS owns a single configured key and no mutable key record, so it
    /// is the backend that must answer every metadata update with a capability
    /// gap rather than a failure of the request.
    async fn static_service() -> ObjectEncryptionService {
        let config =
            KmsConfig::static_kms("static-key".to_string(), base64::engine::general_purpose::STANDARD.encode([0x42u8; 32]));
        let backend = Arc::new(
            StaticKmsBackend::new(config.clone())
                .await
                .expect("static backend should build"),
        );
        ObjectEncryptionService::new(KmsManager::new(backend, config))
    }

    async fn create_key(service: &ObjectEncryptionService, key_name: &str) -> String {
        service
            .create_key(CreateKeyRequest {
                key_name: Some(key_name.to_string()),
                ..Default::default()
            })
            .await
            .expect("key should be created")
            .key_id
    }

    fn metadata(response: &KmsKeyMetadataResponse) -> &KeyMetadata {
        response
            .key_metadata
            .as_ref()
            .expect("metadata response should echo key metadata")
    }

    fn tags(response: &KmsKeyMetadataResponse) -> &HashMap<String, String> {
        &metadata(response).tags
    }

    #[tokio::test]
    async fn description_update_is_visible_to_a_following_describe() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let service = local_service(&temp_dir).await;
        let key_id = create_key(&service, "description-round-trip").await;

        let (status, response, error) =
            execute_metadata_update(&service, &key_id, MetadataUpdate::Description("payments ledger key".to_string())).await;
        assert_eq!(status, StatusCode::OK);
        assert!(response.success);
        assert!(error.is_none());
        assert_eq!(metadata(&response).description.as_deref(), Some("payments ledger key"));

        // The echo must come from the backend, not from the request: a separate
        // describe has to observe the same value.
        let described = service
            .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect("describe should succeed");
        assert_eq!(described.key_metadata.description.as_deref(), Some("payments ledger key"));

        // An empty description clears the stored value rather than storing "".
        let (status, response, _) = execute_metadata_update(&service, &key_id, MetadataUpdate::Description(String::new())).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(metadata(&response).description, None);
    }

    #[tokio::test]
    async fn tagging_round_trips_and_untagging_is_idempotent() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let service = local_service(&temp_dir).await;
        let key_id = create_key(&service, "tag-round-trip").await;

        let (status, response, _) = execute_metadata_update(
            &service,
            &key_id,
            MetadataUpdate::SetTags(HashMap::from([
                ("env".to_string(), "prod".to_string()),
                ("owner".to_string(), "payments".to_string()),
            ])),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(tags(&response).get("env").map(String::as_str), Some("prod"));
        assert_eq!(tags(&response).get("owner").map(String::as_str), Some("payments"));

        // A second call overwrites only the tags it names.
        let (status, response, _) = execute_metadata_update(
            &service,
            &key_id,
            MetadataUpdate::SetTags(HashMap::from([("env".to_string(), "staging".to_string())])),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(tags(&response).get("env").map(String::as_str), Some("staging"));
        assert_eq!(tags(&response).get("owner").map(String::as_str), Some("payments"));

        let (status, response, _) =
            execute_metadata_update(&service, &key_id, MetadataUpdate::RemoveTags(vec!["env".to_string()])).await;
        assert_eq!(status, StatusCode::OK);
        assert!(!tags(&response).contains_key("env"));
        assert_eq!(tags(&response).get("owner").map(String::as_str), Some("payments"));

        // Removing a tag that is not set is a no-op, not an error.
        let (status, response, error) = execute_metadata_update(
            &service,
            &key_id,
            MetadataUpdate::RemoveTags(vec!["env".to_string(), "never-set".to_string()]),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(response.success);
        assert!(error.is_none());
        assert!(!tags(&response).contains_key("env"));
        assert_eq!(tags(&response).get("owner").map(String::as_str), Some("payments"));
    }

    /// The `name` tag is the key's identity: rewriting or dropping it would
    /// leave the key addressable under an id its own metadata no longer states.
    #[tokio::test]
    async fn rewriting_or_dropping_the_name_tag_is_rejected_as_a_bad_request() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let service = local_service(&temp_dir).await;
        let key_id = create_key(&service, "name-tag-guard").await;

        let (status, response, _) = execute_metadata_update(
            &service,
            &key_id,
            MetadataUpdate::SetTags(HashMap::from([("env".to_string(), "prod".to_string())])),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let before = tags(&response).clone();

        for update in [
            MetadataUpdate::SetTags(HashMap::from([("name".to_string(), "impostor".to_string())])),
            MetadataUpdate::RemoveTags(vec!["name".to_string()]),
        ] {
            let (status, response, error) = execute_metadata_update(&service, &key_id, update.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{update:?} must be rejected as a bad request");
            assert_ne!(status, StatusCode::NOT_IMPLEMENTED);
            assert!(!response.success);
            assert!(matches!(error, Some(KmsError::InvalidOperation { .. })));
        }

        // A rejected update leaves the stored tags exactly as they were, so the
        // key stays addressable under the id its metadata states.
        let described = service
            .describe_key(DescribeKeyRequest { key_id: key_id.clone() })
            .await
            .expect("describe should succeed");
        assert_eq!(described.key_metadata.tags, before);
    }

    #[tokio::test]
    async fn a_backend_without_metadata_updates_answers_not_implemented() {
        let service = static_service().await;

        for update in [
            MetadataUpdate::Description("anything".to_string()),
            MetadataUpdate::SetTags(HashMap::from([("env".to_string(), "prod".to_string())])),
            MetadataUpdate::RemoveTags(vec!["env".to_string()]),
        ] {
            let (status, response, error) = execute_metadata_update(&service, "static-key", update.clone()).await;
            assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{update:?} must report the capability gap");
            assert_ne!(status, StatusCode::NOT_FOUND);
            assert!(!response.success);
            assert!(matches!(error, Some(KmsError::UnsupportedCapability { .. })));
        }
    }

    #[tokio::test]
    async fn metadata_update_on_a_missing_key_maps_to_not_found() {
        let temp_dir = tempfile::tempdir().expect("temp dir");
        let service = local_service(&temp_dir).await;

        let (status, response, _) =
            execute_metadata_update(&service, "no-such-key", MetadataUpdate::Description("ghost".to_string())).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(!response.success);
    }

    #[test]
    fn metadata_error_status_contract() {
        let unsupported = KmsError::unsupported_capability("static", "tag_key");
        assert_eq!(metadata_error_status(&unsupported), StatusCode::NOT_IMPLEMENTED);
        assert_ne!(metadata_error_status(&unsupported), StatusCode::NOT_FOUND);

        assert_eq!(
            metadata_error_status(&KmsError::invalid_parameter("reserved tag")),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(metadata_error_status(&KmsError::key_not_found("gone")), StatusCode::NOT_FOUND);
    }

    #[test]
    fn metadata_auth_actions_use_dedicated_kms_actions() {
        assert_eq!(
            kms_update_key_description_actions(),
            vec![Action::KmsAction(KmsAction::UpdateKeyDescriptionAction)]
        );
        assert_eq!(kms_tag_key_actions(), vec![Action::KmsAction(KmsAction::TagResourceAction)]);
        assert_eq!(kms_untag_key_actions(), vec![Action::KmsAction(KmsAction::UntagResourceAction)]);

        for actions in [
            kms_update_key_description_actions(),
            kms_tag_key_actions(),
            kms_untag_key_actions(),
        ] {
            assert!(!actions.contains(&Action::AdminAction(AdminAction::ServerInfoAdminAction)));
            assert!(!actions.contains(&Action::KmsAction(KmsAction::ConfigureAction)));
        }
    }

    #[test]
    fn metadata_request_bodies_reject_unknown_fields() {
        let cases = [
            serde_json::from_str::<UpdateKmsKeyDescriptionRequest>(r#"{"key_id":"key","description":"d","unexpected":true}"#)
                .map(|_| ()),
            serde_json::from_str::<TagKmsKeyRequest>(r#"{"key_id":"key","tags":{},"unexpected":true}"#).map(|_| ()),
            serde_json::from_str::<UntagKmsKeyRequest>(r#"{"key_id":"key","tag_keys":[],"unexpected":true}"#).map(|_| ()),
        ];

        for result in cases {
            let error = result.expect_err("unknown metadata request field should fail");
            assert!(error.to_string().contains("unknown field"));
        }
    }

    /// Each endpoint must authorize against the key its body names, resolved
    /// exactly the way the parser resolves it for execution.
    #[test]
    fn metadata_requests_authorize_against_the_key_they_operate_on() {
        let src = include_str!("kms_key_metadata.rs");
        let handler = src
            .split_once("async fn handle_metadata_request(")
            .expect("metadata entry point should exist")
            .1;
        let handler = &handler[..handler.find("\n/// Replace a KMS key's description").unwrap_or(handler.len())];

        assert!(
            handler.contains("validate_admin_request_with_kms_key("),
            "metadata requests must scope authorization to the requested key"
        );
        assert!(
            handler.contains("scoped_key_id(body, &req.uri)"),
            "authorization must resolve the key exactly as execution does"
        );

        let uri: hyper::Uri = "/rustfs/admin/v3/kms/keys/tag".parse().expect("uri should parse");
        for (body, parse) in [
            (r#"{"key_id":"body-key","description":"d"}"#, parse_update_description as ParseUpdate),
            (r#"{"key_id":"body-key","tags":{"env":"prod"}}"#, parse_tag_key as ParseUpdate),
            (r#"{"key_id":"body-key","tag_keys":["env"]}"#, parse_untag_key as ParseUpdate),
        ] {
            assert_eq!(scoped_key_id(body.as_bytes(), &uri).as_deref(), Some("body-key"));
            assert_eq!(
                scoped_key_id(body.as_bytes(), &uri).as_deref(),
                Some(parse(body.as_bytes()).expect("body should parse").0.as_str())
            );
        }
    }
}
