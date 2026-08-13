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

//! KMS key management admin API handlers

use super::kms_audit::{KmsAdminAudit, KmsAdminOperation};
use crate::admin::auth::{validate_admin_request, validate_admin_request_with_kms_key};
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{current_kms_runtime_service_manager, current_or_init_kms_runtime_service_manager};
use crate::auth::{check_key_valid, get_session_token};
use crate::kms_deletion_gate::current_key_impact;
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use base64::Engine;
use hyper::{HeaderMap, Method, StatusCode};
use matchit::Params;
use rustfs_config::MAX_ADMIN_REQUEST_BODY_SIZE;
use rustfs_kms::{KeyImpactReport, KmsAuditOperation, KmsError, types::*};
use rustfs_policy::policy::action::{Action, KmsAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use tracing::{error, info};

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_KMS_KEYS: &str = "kms_keys";
const EVENT_ADMIN_KMS_KEYS_STATE: &str = "admin_kms_keys_state";

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateKmsKeyRequest {
    pub key_usage: Option<KeyUsage>,
    pub description: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateKmsKeyResponse {
    pub success: bool,
    pub message: String,
    pub key_id: String,
    pub key_metadata: Option<KeyMetadata>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateKeyApiRequest {
    pub key_usage: Option<KeyUsage>,
    pub description: Option<String>,
    pub tags: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateKeyApiResponse {
    pub key_id: String,
    pub key_metadata: KeyMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DescribeKeyApiResponse {
    pub key_metadata: KeyMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListKeysApiResponse {
    pub keys: Vec<KeyInfo>,
    pub truncated: bool,
    pub next_marker: Option<String>,
    /// Identifiers present in the key store that this build could not describe.
    ///
    /// Omitted when empty, so a healthy listing is byte-identical to what it was
    /// before this field existed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unreadable_key_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GenerateDataKeyApiRequest {
    pub key_id: String,
    pub key_spec: KeySpec,
    pub encryption_context: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenerateDataKeyApiResponse {
    pub key_id: String,
    pub plaintext_key: String,   // Base64 encoded
    pub ciphertext_blob: String, // Base64 encoded
}

/// The query parameters of an admin KMS request.
///
/// Parsed with `form_urlencoded`, as the rest of the admin surface does, so a
/// parameter written without a value (`?status`) arrives as an empty value
/// rather than disappearing: a validated parameter must be able to tell "not
/// asked for" from "asked for, unreadable".
pub(super) fn extract_query_params(uri: &hyper::Uri) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if let Some(query) = uri.query() {
        for (key, value) in url::form_urlencoded::parse(query.as_bytes()) {
            params.insert(key.into_owned(), value.into_owned());
        }
    }
    params
}

/// Status values a `status` filter may name, spelled as the response spells
/// them.
const KEY_STATUS_FILTERS: &[(&str, KeyStatus)] = &[
    ("Active", KeyStatus::Active),
    ("Disabled", KeyStatus::Disabled),
    ("PendingDeletion", KeyStatus::PendingDeletion),
    ("Deleted", KeyStatus::Deleted),
];

/// Usage values a `usage` filter may name, spelled as the response spells them.
const KEY_USAGE_FILTERS: &[(&str, KeyUsage)] = &[
    ("EncryptDecrypt", KeyUsage::EncryptDecrypt),
    ("SignVerify", KeyUsage::SignVerify),
];

/// The status and usage filters of a key listing.
struct KeyListFilters {
    status: Option<KeyStatus>,
    usage: Option<KeyUsage>,
}

/// A filter value with casing and word separators folded away, so the spelling
/// the response uses (`PendingDeletion`) and the AWS-shaped one
/// (`PENDING_DELETION`) name the same filter.
fn canonical_filter_value(value: &str) -> String {
    value
        .chars()
        .filter(|character| !matches!(character, '_' | '-'))
        .map(|character| character.to_ascii_lowercase())
        .collect()
}

fn parse_key_filter<T: Clone>(name: &str, value: &str, accepted: &[(&str, T)]) -> Result<T, String> {
    let wanted = canonical_filter_value(value);
    accepted
        .iter()
        .find(|(spelling, _)| canonical_filter_value(spelling) == wanted)
        .map(|(_, filter)| filter.clone())
        .ok_or_else(|| {
            let spellings: Vec<&str> = accepted.iter().map(|(spelling, _)| *spelling).collect();
            format!("invalid value for '{name}': expected one of {}, got '{value}'", spellings.join(", "))
        })
}

/// The `status` and `usage` filters a key listing was asked to apply.
///
/// An absent parameter means "no filter". A parameter that is present but names
/// no known status or usage — an empty value included — is refused rather than
/// dropped: a listing that ignored the filter would answer "show me the keys
/// pending deletion" with every key there is, and nothing in the response would
/// tell the caller that the narrowing never happened.
///
/// The filters narrow a page after the backend has cut it, so a filtered page
/// can be shorter than `limit` — even empty — while `truncated` is still true.
/// Callers must page until `truncated` is false rather than until a page comes
/// back short.
fn key_list_filters(query_params: &HashMap<String, String>) -> Result<KeyListFilters, String> {
    Ok(KeyListFilters {
        status: query_params
            .get("status")
            .map(|value| parse_key_filter("status", value, KEY_STATUS_FILTERS))
            .transpose()?,
        usage: query_params
            .get("usage")
            .map(|value| parse_key_filter("usage", value, KEY_USAGE_FILTERS))
            .transpose()?,
    })
}

/// Read the `limit` query parameter of a key listing.
///
/// An absent parameter means "let the backend apply its default"; a parameter
/// that is present but is not a page size is refused for the same reason a
/// misspelled `status` is. Falling back to the default on `limit=abc` answered
/// "give me everything" with the first hundred keys and put nothing in the
/// response to say the request had not been understood.
/// A well-formed page size larger than `u32` is saturated rather than refused:
/// the service caps every page anyway, so `limit=5000000000` and `limit=5000`
/// mean the same thing, and rejecting only the larger of the two would be an
/// arbitrary line the contract does not draw.
fn parse_list_limit(query_params: &HashMap<String, String>) -> Result<Option<u32>, String> {
    let Some(raw) = query_params.get("limit") else {
        return Ok(None);
    };
    raw.parse::<u64>()
        .map(|limit| Some(u32::try_from(limit).unwrap_or(u32::MAX)))
        .map_err(|_| format!("invalid limit '{raw}': expected a non-negative integer"))
}

fn extract_key_id(uri: &hyper::Uri) -> Option<String> {
    let query_params = extract_query_params(uri);
    ["keyId", "key-id", "key"]
        .into_iter()
        .find_map(|name| query_params.get(name).filter(|value| !value.is_empty()).cloned())
}

/// The `key_id` of a KMS admin request body, read without committing to the
/// strict schema of the endpoint: the authorization gate needs the target key
/// before the body is parsed for execution, and a body that fails the strict
/// parse is rejected right after the gate anyway.
#[derive(Deserialize)]
struct KeyIdProbe {
    #[serde(default)]
    key_id: String,
}

/// Target key of an endpoint that accepts either a JSON body or a `keyId` query
/// parameter, resolved exactly the way the endpoint resolves it for execution so
/// the authorized key can never differ from the operated one. Returns `None`
/// when the target cannot be determined, which authorizes unscoped and lets the
/// request fail on its own parse error afterwards.
pub(super) fn scoped_key_id(body: &[u8], uri: &hyper::Uri) -> Option<String> {
    if body.is_empty() {
        return extract_query_params(uri).get("keyId").cloned();
    }

    serde_json::from_slice::<KeyIdProbe>(body).ok().map(|probe| probe.key_id)
}

fn kms_service_manager_from_context() -> Option<std::sync::Arc<rustfs_kms::KmsServiceManager>> {
    current_kms_runtime_service_manager()
}

async fn kms_encryption_service_from_context() -> Option<std::sync::Arc<rustfs_kms::ObjectEncryptionService>> {
    let manager = current_or_init_kms_runtime_service_manager();
    manager.get_encryption_service().await
}

fn kms_create_key_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::ConfigureAction)]
}

fn kms_describe_key_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::DescribeKeyAction)]
}

fn kms_list_keys_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::ListKeysAction)]
}

fn kms_generate_data_key_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::GenerateDataKeyAction)]
}

fn kms_delete_key_actions() -> Vec<Action> {
    vec![Action::KmsAction(KmsAction::DeleteKeyAction)]
}

pub fn register_kms_key_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys").as_str(),
        AdminOperation(&CreateKmsKeyHandler {}),
    )?;

    r.insert(
        Method::DELETE,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/delete").as_str(),
        AdminOperation(&DeleteKmsKeyHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/cancel-deletion").as_str(),
        AdminOperation(&CancelKmsKeyDeletionHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys").as_str(),
        AdminOperation(&ListKmsKeysHandler {}),
    )?;

    r.insert(
        Method::GET,
        format!("{}{}", ADMIN_PREFIX, "/v3/kms/keys/{key_id}").as_str(),
        AdminOperation(&DescribeKmsKeyHandler {}),
    )?;

    Ok(())
}

/// Create a new KMS master key (legacy endpoint)
pub struct CreateKeyHandler {}

#[async_trait::async_trait]
impl Operation for CreateKeyHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate(
            validate_admin_request(
                &req.headers,
                &cred,
                owner,
                false,
                kms_create_key_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
            )
            .await,
            KmsAuditOperation::CreateKey,
            None,
        )?;

        let body = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request: CreateKeyApiRequest = if body.is_empty() {
            CreateKeyApiRequest {
                key_usage: Some(KeyUsage::EncryptDecrypt),
                description: None,
                tags: None,
            }
        } else {
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
        };

        let Some(service) = kms_encryption_service_from_context().await else {
            return Err(s3_error!(InternalError, "kms service is not initialized"));
        };

        // Extract key name from tags if provided
        let tags = request.tags.unwrap_or_default();
        let key_name = tags.get("name").cloned();

        let kms_request = CreateKeyRequest {
            key_name,
            key_usage: request.key_usage.unwrap_or(KeyUsage::EncryptDecrypt),
            description: request.description,
            tags,
            origin: Some("AWS_KMS".to_string()),
            policy: None,
        };

        match service.create_key_with_context(kms_request, audit.context()).await {
            Ok(response) => {
                let api_response = CreateKeyApiResponse {
                    key_id: response.key_id,
                    key_metadata: response.key_metadata,
                };

                let data = serde_json::to_vec(&api_response)
                    .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "create_key",
                    result = "failed",
                    error = %e,
                    "admin kms keys state"
                );
                Err(s3_error!(InternalError, "failed to create key: {}", e))
            }
        }
    }
}

/// Describe a KMS key (legacy endpoint)
pub struct DescribeKeyHandler {}

#[async_trait::async_trait]
impl Operation for DescribeKeyHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let requested_key_id = extract_key_id(&req.uri);
        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate(
            validate_admin_request_with_kms_key(
                &req.headers,
                &cred,
                owner,
                false,
                kms_describe_key_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                requested_key_id.as_deref().unwrap_or_default(),
            )
            .await,
            KmsAuditOperation::DescribeKey,
            requested_key_id.as_deref(),
        )?;

        let Some(key_id) = requested_key_id else {
            return Err(s3_error!(InvalidRequest, "missing required parameter: 'keyId'"));
        };

        let Some(service) = kms_encryption_service_from_context().await else {
            return Err(s3_error!(InternalError, "kms service is not initialized"));
        };

        let request = DescribeKeyRequest { key_id: key_id.clone() };

        match service.describe_key_with_context(request, audit.context()).await {
            Ok(response) => {
                let api_response = DescribeKeyApiResponse {
                    key_metadata: response.key_metadata,
                };

                let data = serde_json::to_vec(&api_response)
                    .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "describe_key",
                    key_id = %key_id,
                    result = "failed",
                    error = %e,
                    "admin kms keys state"
                );
                Err(s3_error!(InternalError, "failed to describe key: {}", e))
            }
        }
    }
}

/// Serialize a KMS admin response into a snapshot-stable JSON value.
///
/// Response bodies carry `HashMap` tags and metadata, whose iteration order
/// varies per run; sorting every object by key is what makes a snapshot of
/// them reproducible. Shared with [`super::kms_key_metadata`], which pins its
/// own response shape the same way.
#[cfg(test)]
pub(super) fn stable_json_value(value: impl serde::Serialize) -> serde_json::Value {
    fn sorted(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Array(values) => serde_json::Value::Array(values.into_iter().map(sorted).collect()),
            serde_json::Value::Object(map) => {
                let mut entries = map.into_iter().collect::<Vec<_>>();
                entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                serde_json::Value::Object(entries.into_iter().map(|(key, value)| (key, sorted(value))).collect())
            }
            value => value,
        }
    }

    sorted(serde_json::to_value(value).expect("admin KMS response should serialize"))
}

#[cfg(test)]
mod tests {
    use super::{
        CancelKmsKeyDeletionRequest, CancelKmsKeyDeletionResponse, CreateKeyApiRequest, CreateKeyApiResponse,
        CreateKmsKeyRequest, CreateKmsKeyResponse, DeleteKmsKeyRequest, DeleteKmsKeyResponse, DescribeKeyApiResponse,
        DescribeKmsKeyResponse, GenerateDataKeyApiRequest, GenerateDataKeyApiResponse, ListKeysApiResponse, ListKmsKeysResponse,
        delete_key_error_status, delete_request_from_query, extract_key_id, extract_query_params, key_impact_if_requested,
        key_list_filters, kms_create_key_actions, kms_delete_key_actions, kms_describe_key_actions,
        kms_generate_data_key_actions, kms_list_keys_actions, parse_list_limit, scoped_key_id, stable_json_value,
        wants_key_impact,
    };
    use http::Uri;
    use hyper::StatusCode;
    use jiff::Zoned;
    use rustfs_kms::{
        KeyImpactReport, KeyInfo, KeyMetadata, KeyReference, KeyReferenceKind, KeyState, KeyStatus, KeyUsage, KmsError,
        ReferenceScope, RotationDueReason,
    };
    use rustfs_policy::policy::action::{Action, AdminAction, KmsAction};
    use rustfs_policy::policy::{Args, Policy};
    use std::collections::HashMap;

    fn assert_has_action(actions: &[Action], action: Action) {
        assert!(actions.contains(&action), "expected action list to contain {action:?}");
    }

    fn assert_lacks_action(actions: &[Action], action: Action) {
        assert!(!actions.contains(&action), "expected action list not to contain {action:?}");
    }

    #[test]
    fn test_extract_key_id_supports_minio_aliases() {
        for (uri, expected) in [
            ("/rustfs/admin/v3/kms/key/status?keyId=legacy-key", "legacy-key"),
            ("/rustfs/admin/v3/kms/key/status?key-id=minio-key", "minio-key"),
            ("/rustfs/admin/v3/kms/key/status?key=fallback-key", "fallback-key"),
        ] {
            let uri: Uri = uri.parse().expect("uri should parse");
            assert_eq!(extract_key_id(&uri).as_deref(), Some(expected));
        }
    }

    #[test]
    fn test_extract_key_id_skips_empty_values_and_uses_next_alias() {
        let uri: Uri = "/rustfs/admin/v3/kms/key/status?keyId=&key-id=minio-key&key=fallback-key"
            .parse()
            .expect("uri should parse");

        assert_eq!(extract_key_id(&uri).as_deref(), Some("minio-key"));
    }

    #[test]
    fn test_extract_key_id_prefers_legacy_name_over_aliases() {
        let uri: Uri = "/rustfs/admin/v3/kms/key/status?keyId=legacy-key&key-id=minio-key&key=fallback-key"
            .parse()
            .expect("uri should parse");

        assert_eq!(extract_key_id(&uri).as_deref(), Some("legacy-key"));
    }
    #[test]
    fn test_extract_key_id_skips_empty_aliases() {
        for (uri, expected) in [
            ("/rustfs/admin/v3/kms/key/status?keyId=&key-id=minio-key", Some("minio-key")),
            ("/rustfs/admin/v3/kms/key/status?keyId=&key-id=&key=fallback-key", Some("fallback-key")),
            ("/rustfs/admin/v3/kms/key/status?keyId=&key-id=&key=", None),
        ] {
            let uri: Uri = uri.parse().expect("uri should parse");
            assert_eq!(extract_key_id(&uri).as_deref(), expected);
        }
    }

    #[test]
    fn kms_key_auth_actions_use_dedicated_kms_actions() {
        assert_eq!(kms_create_key_actions(), vec![Action::KmsAction(KmsAction::ConfigureAction)]);
        assert_eq!(kms_describe_key_actions(), vec![Action::KmsAction(KmsAction::DescribeKeyAction)]);
        assert_eq!(kms_list_keys_actions(), vec![Action::KmsAction(KmsAction::ListKeysAction)]);
    }

    #[test]
    fn kms_sensitive_key_actions_reject_server_info_fallback() {
        for actions in [kms_generate_data_key_actions(), kms_delete_key_actions()] {
            assert_lacks_action(&actions, Action::AdminAction(AdminAction::ServerInfoAdminAction));
        }

        assert_has_action(&kms_generate_data_key_actions(), Action::KmsAction(KmsAction::GenerateDataKeyAction));
        assert_has_action(&kms_delete_key_actions(), Action::KmsAction(KmsAction::DeleteKeyAction));
    }

    #[test]
    fn kms_key_request_bodies_reject_unknown_fields() {
        let cases = [
            serde_json::from_str::<CreateKeyApiRequest>(r#"{"unexpected_field":true}"#).map(|_| ()),
            serde_json::from_str::<CreateKmsKeyRequest>(r#"{"unexpected_field":true}"#).map(|_| ()),
            serde_json::from_str::<GenerateDataKeyApiRequest>(r#"{"key_id":"key","key_spec":"Aes256","unexpected_field":true}"#)
                .map(|_| ()),
            serde_json::from_str::<DeleteKmsKeyRequest>(r#"{"key_id":"key","unexpected_field":true}"#).map(|_| ()),
            serde_json::from_str::<CancelKmsKeyDeletionRequest>(r#"{"key_id":"key","unexpected_field":true}"#).map(|_| ()),
        ];

        for result in cases {
            let err = result.expect_err("unknown KMS key request field should fail");
            assert!(err.to_string().contains("unknown field"));
        }
    }

    fn delete_query(query: &str) -> Result<DeleteKmsKeyRequest, Box<DeleteKmsKeyResponse>> {
        let uri: Uri = format!("/rustfs/admin/v3/kms/keys/delete?{query}")
            .parse()
            .expect("uri should parse");
        delete_request_from_query(&uri)
    }

    /// The query string can no longer ask for immediate deletion in any form
    /// (rustfs/backlog#1585). Refusing beats downgrading to a scheduled
    /// deletion: a caller who asked for destruction must not read the answer as
    /// "destroyed".
    #[test]
    fn delete_query_cannot_request_immediate_deletion() {
        for query in [
            "keyId=key-a&force_immediate=true",
            "keyId=key-a&force_immediate=True",
            "keyId=key-a&force_immediate=1",
            "keyId=key-a&force_immediate=",
            "keyId=key-a&confirm_key_id=key-a",
            "keyId=key-a&force_immediate=false&confirm_key_id=key-a",
        ] {
            let Err(refused) = delete_query(query) else {
                panic!("{query} must be refused");
            };
            assert!(!refused.success, "{query} must not report success");
            assert_eq!(refused.key_id, "key-a");
            assert!(refused.deletion_date.is_none(), "{query} must not report a deletion date");
            assert!(
                refused.message.contains("JSON body"),
                "{query} must point the caller at the body form: {}",
                refused.message
            );
        }
    }

    /// The scheduled path keeps its query form, including the explicit
    /// `force_immediate=false` some clients send.
    #[test]
    fn delete_query_still_schedules_a_deletion() {
        for (query, expected_window) in [
            ("keyId=key-a", None),
            ("keyId=key-a&force_immediate=false", None),
            ("keyId=key-a&pending_window_in_days=7", Some(7)),
        ] {
            let request = delete_query(query).expect("a scheduled deletion must still parse from the query");
            assert_eq!(request.key_id, "key-a");
            assert_eq!(request.pending_window_in_days, expected_window);
            assert_eq!(request.force_immediate, None, "{query} must reach the service as scheduled");
            assert_eq!(request.confirm_key_id, None, "{query} must not carry a confirmation");
        }
    }

    #[test]
    fn delete_query_without_a_key_id_is_refused() {
        let uri: Uri = "/rustfs/admin/v3/kms/keys/delete".parse().expect("uri should parse");
        let refused = delete_request_from_query(&uri).expect_err("a delete without a key must be refused");
        assert!(refused.message.contains("keyId"));
        assert!(refused.key_id.is_empty());
    }

    /// The body form still carries both immediate-deletion fields; the service
    /// gate, not the transport, decides whether they are honoured.
    #[test]
    fn delete_body_still_carries_the_confirmation_fields() {
        let request =
            serde_json::from_str::<DeleteKmsKeyRequest>(r#"{"key_id":"key-a","force_immediate":true,"confirm_key_id":"key-a"}"#)
                .expect("delete body should parse");

        assert_eq!(request.force_immediate, Some(true));
        assert_eq!(request.confirm_key_id.as_deref(), Some("key-a"));
    }

    /// A refused waiting window and a refused immediate deletion are the
    /// caller's input to fix, so the endpoint answers 400 rather than blaming
    /// the backend.
    #[test]
    fn refused_deletions_report_a_client_error() {
        for error in [
            KmsError::invalid_parameter("pending_window_in_days must be between 7 and 30"),
            KmsError::invalid_operation("immediate deletion of key key-a is not allowed"),
            KmsError::validation_error("bad input"),
        ] {
            assert_eq!(delete_key_error_status(&error), StatusCode::BAD_REQUEST, "{error} must be a 400");
        }

        assert_eq!(delete_key_error_status(&KmsError::key_not_found("key-a")), StatusCode::NOT_FOUND);
        assert_eq!(
            delete_key_error_status(&KmsError::backend_error("vault is down")),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    /// A key the deployment still points at is refused with 409, not 400: the
    /// request is well formed and the key exists, and what has to change to
    /// make it succeed is the configuration, not the request.
    #[test]
    fn a_still_referenced_key_reports_a_conflict() {
        let error = KmsError::key_still_referenced("key-a", vec!["bucket:sse-bucket".to_string()]);
        assert_eq!(delete_key_error_status(&error), StatusCode::CONFLICT);
    }

    #[test]
    fn scoped_key_id_reads_the_body_first_and_falls_back_to_the_query() {
        let uri: Uri = "/rustfs/admin/v3/kms/keys/delete?keyId=query-key"
            .parse()
            .expect("uri should parse");

        assert_eq!(scoped_key_id(br#"{"key_id":"body-key"}"#, &uri).as_deref(), Some("body-key"));
        assert_eq!(scoped_key_id(b"", &uri).as_deref(), Some("query-key"));

        // The query aliases accepted by the legacy describe endpoint are not
        // accepted here, because the handlers only execute on `keyId`.
        let alias_uri: Uri = "/rustfs/admin/v3/kms/keys/delete?key-id=query-key"
            .parse()
            .expect("uri should parse");
        assert_eq!(scoped_key_id(b"", &alias_uri), None);
    }

    /// A body the endpoint will reject leaves the request unscoped: the gate then
    /// answers as it did before resource scoping and the request still fails on
    /// its own parse error, so no key is operated on under that decision.
    #[test]
    fn scoped_key_id_is_absent_for_bodies_that_name_no_key() {
        let uri: Uri = "/rustfs/admin/v3/kms/keys/delete".parse().expect("uri should parse");

        assert_eq!(scoped_key_id(b"not json", &uri), None);
        assert_eq!(scoped_key_id(br#"{"key_id":"" }"#, &uri).as_deref(), Some(""));
        assert_eq!(scoped_key_id(b"{}", &uri).as_deref(), Some(""));
        assert_eq!(scoped_key_id(b"", &uri), None);
    }

    /// Identity policy scoped to a single KMS key, or unscoped when `key_arn` is
    /// `None` (the legacy action-only form that must keep matching every key).
    fn kms_policy(key_arn: Option<&str>) -> Policy {
        let resource = match key_arn {
            Some(arn) => format!(r#","Resource":["{arn}"]"#),
            None => String::new(),
        };
        let document = format!(r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Action":["kms:*"]{resource}}}]}}"#);

        Policy::parse_config(document.as_bytes()).expect("kms policy should parse")
    }

    async fn policy_allows(policy: &Policy, action: Action, key_id: &str, is_owner: bool) -> bool {
        let groups = None;
        let conditions = HashMap::new();
        let claims = HashMap::new();

        policy
            .is_allowed(&Args {
                account: "kms-operator",
                groups: &groups,
                action,
                // The admin gate scopes KMS requests through `AdminResourceScope::kms_key`,
                // which puts the requested key id in the object slot with an empty bucket.
                bucket: "",
                conditions: &conditions,
                is_owner,
                object: key_id,
                claims: &claims,
                deny_only: false,
            })
            .await
    }

    fn describe_by_query(key_id: &str) -> String {
        let uri: Uri = format!("/rustfs/admin/v3/kms/key/status?keyId={key_id}")
            .parse()
            .expect("uri should parse");
        extract_key_id(&uri).unwrap_or_default()
    }

    fn describe_by_path(key_id: &str) -> String {
        // `DescribeKmsKeyHandler` reads the router path parameter verbatim.
        key_id.to_string()
    }

    fn generate_data_key_body(key_id: &str) -> String {
        let body = format!(r#"{{"key_id":"{key_id}","key_spec":"Aes256"}}"#);
        serde_json::from_str::<GenerateDataKeyApiRequest>(&body)
            .expect("generate-data-key body should parse")
            .key_id
    }

    fn key_id_body(key_id: &str) -> String {
        let uri: Uri = "/rustfs/admin/v3/kms/keys".parse().expect("uri should parse");
        scoped_key_id(format!(r#"{{"key_id":"{key_id}"}}"#).as_bytes(), &uri).unwrap_or_default()
    }

    fn key_id_query(key_id: &str) -> String {
        let uri: Uri = format!("/rustfs/admin/v3/kms/keys?keyId={key_id}")
            .parse()
            .expect("uri should parse");
        scoped_key_id(b"", &uri).unwrap_or_default()
    }

    /// Endpoint label, the action it gates on, and the resolution its handler
    /// performs to obtain the key the gate is scoped to.
    type SingleKeyEndpoint = (&'static str, Action, fn(&str) -> String);

    /// Every KMS admin endpoint that acts on one key. Lifecycle endpoints share
    /// `scoped_key_id` with the delete/cancel endpoints (see `kms_key_lifecycle`).
    fn single_key_endpoints() -> Vec<SingleKeyEndpoint> {
        vec![
            (
                "GET /v3/kms/key/status",
                Action::KmsAction(KmsAction::DescribeKeyAction),
                describe_by_query,
            ),
            (
                "GET /v3/kms/keys/{key_id}",
                Action::KmsAction(KmsAction::DescribeKeyAction),
                describe_by_path,
            ),
            (
                "POST /v3/kms/generate-data-key",
                Action::KmsAction(KmsAction::GenerateDataKeyAction),
                generate_data_key_body,
            ),
            ("DELETE /v3/kms/keys/delete", Action::KmsAction(KmsAction::DeleteKeyAction), key_id_body),
            (
                "DELETE /v3/kms/keys/delete?keyId=",
                Action::KmsAction(KmsAction::DeleteKeyAction),
                key_id_query,
            ),
            (
                "POST /v3/kms/keys/cancel-deletion",
                Action::KmsAction(KmsAction::DeleteKeyAction),
                key_id_body,
            ),
            ("POST /v3/kms/keys/enable", Action::KmsAction(KmsAction::EnableKeyAction), key_id_body),
            ("POST /v3/kms/keys/disable", Action::KmsAction(KmsAction::DisableKeyAction), key_id_body),
            ("POST /v3/kms/keys/rotate", Action::KmsAction(KmsAction::RotateKeyAction), key_id_body),
            (
                "POST /v3/kms/keys/update-description",
                Action::KmsAction(KmsAction::UpdateKeyDescriptionAction),
                key_id_body,
            ),
            ("POST /v3/kms/keys/tag", Action::KmsAction(KmsAction::TagResourceAction), key_id_body),
            ("POST /v3/kms/keys/untag", Action::KmsAction(KmsAction::UntagResourceAction), key_id_body),
        ]
    }

    /// A policy limited to `key/A` must not authorize any single-key endpoint
    /// against another key (rustfs/backlog#1582).
    #[tokio::test]
    async fn single_key_endpoints_reject_a_key_outside_the_policy_scope() {
        let policy = kms_policy(Some("arn:aws:kms:::key/key-a"));

        for (endpoint, action, resolve_key_id) in single_key_endpoints() {
            assert!(
                policy_allows(&policy, action, &resolve_key_id("key-a"), false).await,
                "{endpoint} must stay allowed on the key the policy names"
            );
            assert!(
                !policy_allows(&policy, action, &resolve_key_id("key-b"), false).await,
                "{endpoint} must be denied on a key outside the policy scope"
            );
        }
    }

    /// Compatibility pin: a KMS statement without resources keeps matching every
    /// key, and the owner short-circuit is unaffected by scoping.
    #[tokio::test]
    async fn unscoped_policies_and_owner_credentials_keep_matching_every_key() {
        let unscoped = kms_policy(None);
        let scoped = kms_policy(Some("arn:aws:kms:::key/key-a"));

        for (endpoint, action, resolve_key_id) in single_key_endpoints() {
            for key in ["key-a", "key-b"] {
                assert!(
                    policy_allows(&unscoped, action, &resolve_key_id(key), false).await,
                    "{endpoint} must stay allowed on {key} for a resource-less KMS statement"
                );
                assert!(
                    policy_allows(&scoped, action, &resolve_key_id(key), true).await,
                    "{endpoint} must stay allowed on {key} for an owner credential"
                );
            }
        }
    }

    /// The key the handler resolves also reaches Deny statements, so an explicit
    /// Deny on one key survives an unscoped Allow.
    #[tokio::test]
    async fn explicit_deny_on_one_key_overrides_an_unscoped_allow() {
        let document = r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["kms:*"]},
            {"Effect":"Deny","Action":["kms:*"],"Resource":["arn:aws:kms:::key/key-b"]}
        ]}"#;
        let policy = Policy::parse_config(document.as_bytes()).expect("kms policy should parse");

        for (endpoint, action, resolve_key_id) in single_key_endpoints() {
            assert!(
                policy_allows(&policy, action, &resolve_key_id("key-a"), false).await,
                "{endpoint} must stay allowed on a key the Deny does not name"
            );
            assert!(
                !policy_allows(&policy, action, &resolve_key_id("key-b"), false).await,
                "{endpoint} must be denied on the key the Deny names"
            );
        }
    }

    /// The single-key handlers must reach the gate through the KMS-scoped entry
    /// point; the endpoints that have no target key must not (passing an empty
    /// scope there would be a no-op, but the unscoped call documents intent).
    #[test]
    fn single_key_handlers_authorize_through_the_kms_scoped_gate() {
        let src = include_str!("kms_keys.rs");

        for handler in [
            "DescribeKeyHandler",
            "GenerateDataKeyHandler",
            "DeleteKmsKeyHandler",
            "CancelKmsKeyDeletionHandler",
            "DescribeKmsKeyHandler",
        ] {
            let block = operation_block(src, handler);
            assert!(
                block.contains("validate_admin_request_with_kms_key("),
                "{handler} must scope its authorization to the requested key"
            );
        }

        for handler in [
            "CreateKeyHandler",
            "ListKeysHandler",
            "CreateKmsKeyHandler",
            "ListKmsKeysHandler",
        ] {
            let block = operation_block(src, handler);
            assert!(
                !block.contains("validate_admin_request_with_kms_key("),
                "{handler} has no target key and must not claim a KMS resource scope"
            );
        }
    }

    fn referenced_impact() -> KeyImpactReport {
        let mut impact = KeyImpactReport::configuration_layer("key-a");
        impact.push_reference(KeyReference {
            kind: KeyReferenceKind::BucketDefaultEncryption,
            id: "sse-bucket".to_string(),
            detail: "bucket default encryption names this key".to_string(),
        });
        impact
    }

    fn fixed_zoned(value: &str) -> Zoned {
        value.parse().expect("snapshot timestamp should parse")
    }

    fn snapshot_key_metadata() -> KeyMetadata {
        KeyMetadata {
            key_id: "key-a".to_string(),
            key_state: KeyState::Enabled,
            key_usage: KeyUsage::EncryptDecrypt,
            description: Some("snapshot key".to_string()),
            creation_date: fixed_zoned("2026-01-01T00:00:00Z[UTC]"),
            deletion_date: Some(fixed_zoned("2026-02-01T00:00:00Z[UTC]")),
            origin: "RUSTFS_KMS".to_string(),
            key_manager: "RUSTFS".to_string(),
            tags: HashMap::from([("name".to_string(), "key-a".to_string())]),
        }
    }

    fn snapshot_key_info() -> KeyInfo {
        KeyInfo {
            key_id: "key-a".to_string(),
            description: Some("snapshot key".to_string()),
            algorithm: "AES_256".to_string(),
            usage: KeyUsage::EncryptDecrypt,
            status: KeyStatus::Active,
            version: 1,
            metadata: HashMap::from([("origin".to_string(), "RUSTFS_KMS".to_string())]),
            tags: HashMap::from([("name".to_string(), "key-a".to_string())]),
            created_at: fixed_zoned("2026-01-01T00:00:00Z[UTC]"),
            rotated_at: Some(fixed_zoned("2026-01-15T00:00:00Z[UTC]")),
            created_by: Some("admin".to_string()),
            // Pinned as a key with a verdict: the empty case is already the
            // default, and only a populated one fixes the wire names.
            rotation_due: true,
            rotation_due_reason: Some(RotationDueReason::Age),
            // `#[serde(skip)]`: populated on purpose so the snapshot proves the
            // wrap counter stays off the admin wire even when a backend set it.
            wrap_budget_reserved: Some(1_000_000),
        }
    }

    /// Every JSON body this module serves, pinned where it is served from.
    ///
    /// `rustfs-kms` owns look-alike response types with the same operation
    /// names and different fields. A snapshot over there cannot fail when one
    /// of the types below changes, so pinning the admin wire contract has to
    /// happen in this crate, next to the types that are actually serialized.
    ///
    /// `Option` fields are covered in both states: none of these types skip a
    /// `None`, and a consumer that reads `impact` or `key_metadata` as
    /// explicit `null` breaks if one later starts being omitted instead.
    #[test]
    fn kms_key_admin_responses_have_stable_json_shapes() {
        insta::assert_json_snapshot!(
            "kms_admin_create_key_response",
            stable_json_value(CreateKmsKeyResponse {
                success: true,
                message: "key created successfully".to_string(),
                key_id: "key-a".to_string(),
                key_metadata: Some(snapshot_key_metadata()),
            })
        );
        insta::assert_json_snapshot!(
            "kms_admin_create_key_api_response",
            stable_json_value(CreateKeyApiResponse {
                key_id: "key-a".to_string(),
                key_metadata: snapshot_key_metadata(),
            })
        );
        insta::assert_json_snapshot!(
            "kms_admin_describe_key_api_response",
            stable_json_value(DescribeKeyApiResponse {
                key_metadata: snapshot_key_metadata(),
            })
        );
        insta::assert_json_snapshot!(
            "kms_admin_list_keys_api_response",
            stable_json_value(ListKeysApiResponse {
                keys: vec![snapshot_key_info()],
                truncated: true,
                next_marker: Some("key-b".to_string()),
                unreadable_key_ids: Vec::new(),
            })
        );
        // The empty case above omits the field, so the wire name clients read
        // is only fixed by a populated one.
        insta::assert_json_snapshot!(
            "kms_admin_list_keys_api_response_with_unreadable_keys",
            stable_json_value(ListKeysApiResponse {
                keys: vec![snapshot_key_info()],
                truncated: false,
                next_marker: None,
                unreadable_key_ids: vec!["key-c".to_string()],
            })
        );
        insta::assert_json_snapshot!(
            "kms_admin_generate_data_key_api_response",
            stable_json_value(GenerateDataKeyApiResponse {
                key_id: "key-a".to_string(),
                plaintext_key: "cGxhaW50ZXh0LXBsYWNlaG9sZGVy".to_string(),
                ciphertext_blob: "Y2lwaGVydGV4dC1wbGFjZWhvbGRlcg==".to_string(),
            })
        );
        insta::assert_json_snapshot!(
            "kms_admin_delete_key_response",
            stable_json_value(DeleteKmsKeyResponse {
                success: true,
                message: "key scheduled for deletion".to_string(),
                key_id: "key-a".to_string(),
                deletion_date: Some("2026-02-01T00:00:00Z".to_string()),
                impact: Some(referenced_impact()),
            })
        );
        insta::assert_json_snapshot!(
            "kms_admin_delete_key_response_without_impact",
            stable_json_value(DeleteKmsKeyResponse {
                success: false,
                message: "key not found".to_string(),
                key_id: "key-a".to_string(),
                deletion_date: None,
                impact: None,
            })
        );
        insta::assert_json_snapshot!(
            "kms_admin_cancel_key_deletion_response",
            stable_json_value(CancelKmsKeyDeletionResponse {
                success: true,
                message: "key deletion canceled".to_string(),
                key_id: "key-a".to_string(),
                key_metadata: Some(snapshot_key_metadata()),
            })
        );
        insta::assert_json_snapshot!(
            "kms_admin_list_keys_response",
            stable_json_value(ListKmsKeysResponse {
                success: true,
                message: "keys listed".to_string(),
                keys: vec![snapshot_key_info()],
                truncated: true,
                next_marker: Some("key-b".to_string()),
                unreadable_key_ids: Vec::new(),
            })
        );
        // Pinned separately because the field is omitted when empty: without a
        // populated case nothing would fix the wire name clients read.
        insta::assert_json_snapshot!(
            "kms_admin_list_keys_response_with_unreadable_keys",
            stable_json_value(ListKmsKeysResponse {
                success: true,
                message: "keys listed".to_string(),
                keys: vec![snapshot_key_info()],
                truncated: false,
                next_marker: None,
                unreadable_key_ids: vec!["key-c".to_string()],
            })
        );
        insta::assert_json_snapshot!(
            "kms_admin_describe_key_response",
            stable_json_value(DescribeKmsKeyResponse {
                success: true,
                message: "key described successfully".to_string(),
                key_metadata: Some(snapshot_key_metadata()),
                impact: Some(referenced_impact()),
            })
        );
        insta::assert_json_snapshot!(
            "kms_admin_describe_key_response_missing",
            stable_json_value(DescribeKmsKeyResponse {
                success: false,
                message: "key not found".to_string(),
                key_metadata: None,
                impact: None,
            })
        );
    }

    /// Scheduling a deletion for a key that bucket configuration still points
    /// at succeeds — it destroys nothing and stays cancellable — but the
    /// caller has to be told, in the same response, what will refuse the
    /// destruction once the window runs out.
    #[test]
    fn a_scheduled_deletion_succeeds_and_still_names_what_will_block_it() {
        let response = DeleteKmsKeyResponse {
            success: true,
            message: "key deleted successfully".to_string(),
            key_id: "key-a".to_string(),
            deletion_date: Some("2026-01-01T00:00:00Z".to_string()),
            impact: Some(referenced_impact()),
        };

        let json = serde_json::to_value(&response).expect("response should serialize");
        assert_eq!(
            json["success"], true,
            "an outstanding reference must not change the outcome of a schedule"
        );
        assert_eq!(json["impact"]["references"][0]["kind"], "bucket-default-encryption");
        assert_eq!(json["impact"]["references"][0]["id"], "sse-bucket");
    }

    /// The impact section is exhaustive only over what it read, and says so.
    /// A caller must be able to tell an empty reference list apart from a
    /// key that nothing uses, because this report cannot distinguish them.
    #[test]
    fn the_impact_section_states_its_coverage_and_claims_no_key_is_unused() {
        let empty = DescribeKmsKeyResponse {
            success: true,
            message: "Key described successfully".to_string(),
            key_metadata: None,
            impact: Some(KeyImpactReport::configuration_layer("key-a")),
        };

        let json = serde_json::to_value(&empty).expect("response should serialize");
        assert_eq!(json["impact"]["references"].as_array().expect("references is an array").len(), 0);
        assert_eq!(json["impact"]["completeness"], "exact");
        assert_eq!(json["impact"]["coverage"]["scanned"][0], "bucket-default-encryption");
        assert_eq!(
            json["impact"]["coverage"]["not_scanned"][0], "object-envelopes",
            "an empty reference list is only honest next to the scopes it did not read"
        );

        let referenced = serde_json::to_value(DeleteKmsKeyResponse {
            success: true,
            message: String::new(),
            key_id: "key-a".to_string(),
            deletion_date: None,
            impact: Some(referenced_impact()),
        })
        .expect("response should serialize");

        for body in [json.to_string(), referenced.to_string()] {
            for forbidden in ["in_use", "unused", "unreferenced", "safe_to_delete", "deletable"] {
                assert!(!body.contains(forbidden), "the impact section must not carry a `{forbidden}` claim");
            }
        }
    }

    /// The impact section is a report, never an input to a decision here. The
    /// deletion worker's fail-closed gate stays the only thing that decides
    /// whether material is destroyed; a handler that branched on this report
    /// would be treating "nothing found in the configuration layer" as
    /// "nothing uses this key", which it is not.
    #[test]
    fn handlers_report_the_key_impact_without_acting_on_it() {
        let src = include_str!("kms_keys.rs");

        // Deleting is the request whose consequences the caller cannot
        // otherwise see, and it is not polled, so it always reports.
        let delete = operation_block(src, "DeleteKmsKeyHandler");
        assert!(
            delete.contains("let impact = current_key_impact("),
            "the delete path must report the configuration impact unconditionally"
        );

        // Describing is polled, so the same collection is opt-in there.
        let describe = operation_block(src, "DescribeKmsKeyHandler");
        assert!(
            describe.contains("key_impact_if_requested(wants_impact,"),
            "the describe path must collect the impact only when the request asked for it"
        );
        assert!(
            !describe.contains("current_key_impact("),
            "the describe path must not reach the collection except through the opt-in"
        );

        // Neither handler may read what the report says. Constructing it and
        // handing it to the response is the whole of their business: a handler
        // that inspected it would be treating "nothing found in the
        // configuration layer" as "nothing uses this key", which it is not.
        for (handler, block) in [("DeleteKmsKeyHandler", delete), ("DescribeKmsKeyHandler", describe)] {
            for inspection in [
                "impact.blocks_destruction",
                "impact.references",
                "impact.completeness",
                "impact.coverage",
                "impact.key_id",
                "match impact",
            ] {
                assert!(!block.contains(inspection), "{handler} must not read the impact report (`{inspection}`)");
            }
        }
    }

    fn list_filters(query: &str) -> Result<(Option<KeyStatus>, Option<KeyUsage>), String> {
        let uri: Uri = format!("/rustfs/admin/v3/kms/keys{query}").parse().expect("uri should parse");
        key_list_filters(&extract_query_params(&uri)).map(|filters| (filters.status, filters.usage))
    }

    /// A filter is either applied or refused. Answering a narrowed listing with
    /// the unfiltered key set looks, from the response alone, exactly like a key
    /// set in which everything matches — the caller cannot tell that its
    /// `status=PendingDeletion` never reached the backend.
    #[test]
    fn a_key_listing_filter_is_either_applied_or_refused() {
        assert_eq!(list_filters(""), Ok((None, None)), "an unfiltered listing must stay unfiltered");
        assert_eq!(
            list_filters("?status=PendingDeletion&usage=EncryptDecrypt"),
            Ok((Some(KeyStatus::PendingDeletion), Some(KeyUsage::EncryptDecrypt)))
        );

        // The spelling the response carries and the AWS-shaped one name the
        // same filter, so a value read off a listing can be sent straight back.
        for query in [
            "?status=pending_deletion",
            "?status=PENDING-DELETION",
            "?status=pendingdeletion",
        ] {
            assert_eq!(list_filters(query), Ok((Some(KeyStatus::PendingDeletion), None)), "{query}");
        }

        // Present but unreadable — including a value-less or empty parameter —
        // is refused instead of read as "no filter".
        for (query, name) in [
            ("?status=enabled", "status"),
            ("?status=", "status"),
            ("?status", "status"),
            ("?usage=encrypt", "usage"),
            ("?usage=", "usage"),
            ("?status=Active&usage=sign", "usage"),
        ] {
            let error = list_filters(query).expect_err("an unreadable filter must be refused");
            assert!(
                error.contains(&format!("invalid value for '{name}'")),
                "unhelpful message for {query}: {error}"
            );
        }
    }

    fn list_limit(query: &str) -> Result<Option<u32>, String> {
        let uri: Uri = format!("/rustfs/admin/v3/kms/keys{query}").parse().expect("uri should parse");
        parse_list_limit(&extract_query_params(&uri))
    }

    /// A page size is either applied or refused, for the same reason a filter
    /// is. Falling back to the default on an unreadable `limit` answered a
    /// request the server had not understood with a full-looking page, and put
    /// nothing in the response to say so.
    #[test]
    fn a_key_listing_limit_is_either_applied_or_refused() {
        assert_eq!(list_limit(""), Ok(None), "an absent limit leaves the backend default in place");
        assert_eq!(list_limit("?limit=25"), Ok(Some(25)));
        // Zero is a well-formed request for an empty page, not a missing value.
        assert_eq!(list_limit("?limit=0"), Ok(Some(0)));

        for query in ["?limit=abc", "?limit=-1", "?limit=", "?limit", "?limit=1.5"] {
            let error = list_limit(query).expect_err("an unreadable limit must be refused");
            assert!(error.contains("invalid limit"), "unhelpful message for {query}: {error}");
        }

        // A well-formed page size wider than `u32` saturates rather than being
        // refused: the service caps every page anyway, so refusing only the
        // larger of two over-the-cap values would be a line the contract does
        // not draw.
        assert_eq!(list_limit("?limit=99999999999999"), Ok(Some(u32::MAX)));
    }

    /// Both list endpoints must read the caller's page size through the strict
    /// parser rather than swallowing a malformed one.
    #[test]
    fn both_list_handlers_refuse_an_unreadable_limit() {
        let src = include_str!("kms_keys.rs");

        for handler in ["ListKeysHandler", "ListKmsKeysHandler"] {
            let block = operation_block(src, handler);
            assert!(
                block.contains("parse_list_limit(&query_params)"),
                "{handler} must read the page size through the strict parser"
            );
            assert!(
                !block.contains("parse::<u32>().ok()"),
                "{handler} must not silently fall back on an unreadable page size"
            );
        }
    }

    /// Both list endpoints must hand the caller's filters to the KMS. Pinning
    /// them to `None` — as both did before they were wired up — answers a
    /// narrowed listing with every key there is.
    #[test]
    fn both_list_handlers_pass_the_requested_filters_to_the_kms() {
        let src = include_str!("kms_keys.rs");

        for handler in ["ListKeysHandler", "ListKmsKeysHandler"] {
            let block = operation_block(src, handler);
            assert!(
                block.contains("key_list_filters(&query_params)"),
                "{handler} must read the requested filters"
            );
            assert!(
                block.contains("status_filter: filters.status") && block.contains("usage_filter: filters.usage"),
                "{handler} must pass the requested filters to the KMS"
            );
        }
    }

    fn describe_uri(query: &str) -> Uri {
        format!("/rustfs/admin/v3/kms/keys/key-a{query}")
            .parse()
            .expect("uri should parse")
    }

    #[test]
    fn the_impact_section_is_opt_in_and_refuses_an_unreadable_opt_in() {
        assert_eq!(
            wants_key_impact(&describe_uri("")),
            Ok(false),
            "the default read path must not ask for it"
        );
        assert_eq!(wants_key_impact(&describe_uri("?impact=false")), Ok(false));
        assert_eq!(wants_key_impact(&describe_uri("?impact=true")), Ok(true));

        // A typo must not be read as "off": the caller asked for the section,
        // and an absent section would be indistinguishable from an empty one.
        for query in ["?impact=maybe", "?impact=1", "?impact=", "?impact=TRUE"] {
            let error = wants_key_impact(&describe_uri(query)).expect_err("a malformed opt-in must be refused");
            assert!(error.contains("expected 'true' or 'false'"), "unhelpful message for {query}: {error}");
        }
    }

    /// A report can only come from the collection, so `None` is proof that the
    /// default read path never reached the bucket listing behind it.
    #[tokio::test]
    async fn the_default_describe_path_never_collects_the_impact() {
        assert!(key_impact_if_requested(false, "key-a", None).await.is_none());

        let collected = key_impact_if_requested(true, "key-a", None)
            .await
            .expect("an opted-in describe must carry the section");
        assert_eq!(collected.key_id, "key-a");
        assert_eq!(
            collected.coverage.not_scanned,
            vec![ReferenceScope::ObjectEnvelopes, ReferenceScope::InProgressMultipartUploads]
        );
    }

    fn operation_block<'a>(src: &'a str, handler: &str) -> &'a str {
        let marker = format!("impl Operation for {handler}");
        let block = src.split_once(&marker).expect("handler impl should exist").1;
        let end = ["\n/// ", "\n#[derive(", "\n#[cfg(test)]", "\npub struct "]
            .into_iter()
            .filter_map(|item| block.find(item))
            .min()
            .unwrap_or(block.len());
        &block[..end]
    }
}

/// List KMS keys (legacy endpoint)
pub struct ListKeysHandler {}

#[async_trait::async_trait]
impl Operation for ListKeysHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate(
            validate_admin_request(
                &req.headers,
                &cred,
                owner,
                false,
                kms_list_keys_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
            )
            .await,
            KmsAuditOperation::ListKeys,
            None,
        )?;

        let query_params = extract_query_params(&req.uri);
        // Validated before the listing runs, so a page size or filter the
        // service cannot apply fails as the input error it is instead of
        // returning the whole key set as if it had been narrowed.
        let (limit, filters) = parse_list_limit(&query_params)
            .and_then(|limit| key_list_filters(&query_params).map(|filters| (limit, filters)))
            .map_err(|message| s3_error!(InvalidArgument, "{}", message))?;
        let marker = query_params.get("marker").cloned();

        let Some(service) = kms_encryption_service_from_context().await else {
            return Err(s3_error!(InternalError, "kms service is not initialized"));
        };

        let request = ListKeysRequest {
            limit,
            marker,
            status_filter: filters.status,
            usage_filter: filters.usage,
        };

        match service.list_keys_with_context(request, audit.context()).await {
            Ok(response) => {
                let api_response = ListKeysApiResponse {
                    keys: response.keys,
                    truncated: response.truncated,
                    next_marker: response.next_marker,
                    unreadable_key_ids: response.unreadable_key_ids,
                };

                let data = serde_json::to_vec(&api_response)
                    .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "list_keys",
                    result = "failed",
                    error = %e,
                    "admin kms keys state"
                );
                Err(s3_error!(InternalError, "failed to list keys: {}", e))
            }
        }
    }
}

/// Generate data encryption key (legacy endpoint)
pub struct GenerateDataKeyHandler {}

#[async_trait::async_trait]
impl Operation for GenerateDataKeyHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        // The body names the key this endpoint derives a data key from, so it has
        // to be read before the gate runs. Input failures stay deferred until
        // after the gate so an unauthorized caller still sees AccessDenied.
        let parsed = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))
            .and_then(|body| {
                serde_json::from_slice::<GenerateDataKeyApiRequest>(&body)
                    .map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))
            });

        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate_admin(
            validate_admin_request_with_kms_key(
                &req.headers,
                &cred,
                owner,
                false,
                kms_generate_data_key_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                parsed.as_ref().map(|request| request.key_id.as_str()).unwrap_or_default(),
            )
            .await,
            KmsAdminOperation::GenerateDataKey,
            parsed.as_ref().ok().map(|request| request.key_id.as_str()),
        )?;

        let request = parsed?;

        let Some(service) = kms_encryption_service_from_context().await else {
            return Err(s3_error!(InternalError, "KMS service not initialized"));
        };

        let key_id = request.key_id.clone();
        let kms_request = GenerateDataKeyRequest {
            key_id: request.key_id,
            key_spec: request.key_spec,
            encryption_context: request.encryption_context.unwrap_or_default(),
        };

        // Audited here rather than in the KMS layer: data-key derivation goes
        // through the encryption service, which has no context-aware entry
        // point, so this is the only place that knows who asked.
        let result = service.generate_data_key(kms_request).await;
        audit.finish(KmsAdminOperation::GenerateDataKey, Some(&key_id), result.as_ref().err());

        match result {
            Ok(response) => {
                let api_response = GenerateDataKeyApiResponse {
                    key_id: response.key_id,
                    plaintext_key: base64::prelude::BASE64_STANDARD.encode(&response.plaintext_key),
                    ciphertext_blob: base64::prelude::BASE64_STANDARD.encode(&response.ciphertext_blob),
                };

                let data = serde_json::to_vec(&api_response)
                    .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "generate_data_key",
                    result = "failed",
                    error = %e,
                    "admin kms keys state"
                );
                Err(s3_error!(InternalError, "failed to generate data key: {}", e))
            }
        }
    }
}

/// Create a new KMS key
pub struct CreateKmsKeyHandler;

#[async_trait::async_trait]
impl Operation for CreateKmsKeyHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate(
            validate_admin_request(
                &req.headers,
                &cred,
                owner,
                false,
                kms_create_key_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
            )
            .await,
            KmsAuditOperation::CreateKey,
            None,
        )?;

        let body = req
            .input
            .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
            .await
            .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request: CreateKmsKeyRequest = if body.is_empty() {
            CreateKmsKeyRequest {
                key_usage: Some(KeyUsage::EncryptDecrypt),
                description: None,
                tags: None,
            }
        } else {
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
        };

        let Some(service_manager) = kms_service_manager_from_context() else {
            let response = CreateKmsKeyResponse {
                success: false,
                message: "kms service manager is not initialized".to_string(),
                key_id: "".to_string(),
                key_metadata: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let Some(manager) = service_manager.get_manager().await else {
            let response = CreateKmsKeyResponse {
                success: false,
                message: "kms service is not running".to_string(),
                key_id: "".to_string(),
                key_metadata: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        // Extract key name from tags if provided
        let tags = request.tags.unwrap_or_default();
        let key_name = tags.get("name").cloned();

        let kms_request = CreateKeyRequest {
            key_name,
            key_usage: request.key_usage.unwrap_or(KeyUsage::EncryptDecrypt),
            description: request.description,
            tags,
            origin: Some("AWS_KMS".to_string()),
            policy: None,
        };

        match manager.create_key_with_context(kms_request, audit.context()).await {
            Ok(kms_response) => {
                info!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "create_key",
                    key_id = %kms_response.key_id,
                    state = "completed",
                    "admin kms keys state"
                );
                let response = CreateKmsKeyResponse {
                    success: true,
                    message: "key created successfully".to_string(),
                    key_id: kms_response.key_id,
                    key_metadata: Some(kms_response.key_metadata),
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "create_key",
                    result = "failed",
                    error = %e,
                    "admin kms keys state"
                );
                let response = CreateKmsKeyResponse {
                    success: false,
                    message: format!("failed to create key: {e}"),
                    key_id: "".to_string(),
                    key_metadata: None,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::INTERNAL_SERVER_ERROR, Body::from(data)), headers))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeleteKmsKeyRequest {
    pub key_id: String,
    pub pending_window_in_days: Option<u32>,
    pub force_immediate: Option<bool>,
    /// Echo of `key_id`, required by the service gate before it will destroy
    /// key material immediately. Absent for an ordinary scheduled deletion.
    pub confirm_key_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteKmsKeyResponse {
    pub success: bool,
    pub message: String,
    pub key_id: String,
    pub deletion_date: Option<String>,
    /// Configuration that still points at the key when the request was
    /// handled. A scheduled deletion succeeds regardless — it destroys
    /// nothing and stays cancellable — but the deletion worker will refuse to
    /// destroy the material while any of these references remain, so an
    /// operator has to be able to see them at the moment they schedule it
    /// rather than only in a server-side log once the window has run out.
    ///
    /// `None` when the request never got far enough to identify a key or
    /// reach the KMS service. See [`KeyImpactReport`] for what an empty
    /// reference list does and does not mean.
    pub impact: Option<KeyImpactReport>,
}

const IMMEDIATE_DELETION_QUERY_RETIRED: &str = "immediate deletion is no longer accepted as a query parameter; send it as a JSON body with force_immediate and confirm_key_id set to the key id";

/// Delete request carried entirely by the query string, which can only ever
/// schedule a deletion.
///
/// Immediate deletion destroys key material outright and takes every object
/// encrypted under the key with it, so it is reachable only through the JSON
/// body form (rustfs/backlog#1585): a URL travels through shell history, proxy
/// logs and browser bars, and asking for it there is far too easy to do by
/// accident. An immediate-deletion attempt made this way is refused rather
/// than downgraded to a scheduled deletion, so the caller cannot mistake one
/// outcome for the other.
///
/// The refusal is boxed because it is a full response body, not an error code:
/// carrying one inline would make every `Ok` of this function as wide as the
/// widest failure it can describe.
fn delete_request_from_query(uri: &hyper::Uri) -> Result<DeleteKmsKeyRequest, Box<DeleteKmsKeyResponse>> {
    let query_params = extract_query_params(uri);
    let refuse = |key_id: &str, message: &str| {
        Box::new(DeleteKmsKeyResponse {
            success: false,
            message: message.to_string(),
            key_id: key_id.to_string(),
            deletion_date: None,
            // The request never reached the KMS service, so nothing was
            // collected; this is not a report that found no references.
            impact: None,
        })
    };

    let Some(key_id) = query_params.get("keyId") else {
        return Err(refuse("", "missing required parameter: 'keyId'"));
    };

    // `force_immediate=false` is the scheduled path spelled out, so it stays
    // accepted; anything else in that parameter is an immediate-deletion
    // attempt, including a value that does not parse as a boolean.
    if query_params.get("force_immediate").is_some_and(|value| value != "false") || query_params.contains_key("confirm_key_id") {
        return Err(refuse(key_id, IMMEDIATE_DELETION_QUERY_RETIRED));
    }

    Ok(DeleteKmsKeyRequest {
        key_id: key_id.clone(),
        pending_window_in_days: query_params.get("pending_window_in_days").and_then(|s| s.parse::<u32>().ok()),
        force_immediate: None,
        confirm_key_id: None,
    })
}

/// Status for a deletion the KMS refused.
///
/// A rejected waiting window and a refused immediate deletion both arrive as
/// [`KmsError::InvalidOperation`], and both are the caller's input to fix, so
/// they must surface as 400 rather than as a server fault.
fn delete_key_error_status(error: &KmsError) -> StatusCode {
    match error {
        KmsError::KeyNotFound { .. } => StatusCode::NOT_FOUND,
        KmsError::InvalidOperation { .. } | KmsError::ValidationError { .. } => StatusCode::BAD_REQUEST,
        // Damaged or missing key material is an integrity fault of an existing
        // key: it must surface as a server error, never as NOT_FOUND (the key
        // exists) and never as a retryable backend outage.
        KmsError::MaterialMissing { .. }
        | KmsError::MaterialCorrupt { .. }
        | KmsError::MaterialAuthenticationFailed { .. }
        | KmsError::UnsupportedFormatVersion { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        // The request was well formed and the key exists; it is the state of
        // the deployment around it that refuses, and that state can change
        // without the caller changing anything about the request.
        KmsError::KeyStillReferenced { .. } => StatusCode::CONFLICT,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Delete a KMS key
pub struct DeleteKmsKeyHandler;

#[async_trait::async_trait]
impl Operation for DeleteKmsKeyHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        // Read the body before the gate so the request can be scoped to the key it
        // targets; the read failure is surfaced afterwards to keep AccessDenied
        // ahead of input errors for callers that are not authorized at all.
        let body = req.input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await;
        let scoped = body.as_ref().ok().and_then(|body| scoped_key_id(body, &req.uri));
        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate(
            validate_admin_request_with_kms_key(
                &req.headers,
                &cred,
                owner,
                false,
                kms_delete_key_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                scoped.as_deref().unwrap_or_default(),
            )
            .await,
            KmsAuditOperation::ScheduleKeyDeletion,
            scoped.as_deref(),
        )?;

        let body = body.map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request: DeleteKmsKeyRequest = if body.is_empty() {
            match delete_request_from_query(&req.uri) {
                Ok(request) => request,
                Err(response) => {
                    let data = serde_json::to_vec(&response)
                        .map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
                    let mut headers = HeaderMap::new();
                    headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
                    return Ok(S3Response::with_headers((StatusCode::BAD_REQUEST, Body::from(data)), headers));
                }
            }
        } else {
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
        };

        let Some(service_manager) = kms_service_manager_from_context() else {
            let response = DeleteKmsKeyResponse {
                success: false,
                message: "kms service manager is not initialized".to_string(),
                key_id: request.key_id,
                deletion_date: None,
                impact: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let Some(manager) = service_manager.get_manager().await else {
            let response = DeleteKmsKeyResponse {
                success: false,
                message: "kms service is not running".to_string(),
                key_id: request.key_id,
                deletion_date: None,
                impact: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let kms_request = DeleteKeyRequest {
            key_id: request.key_id.clone(),
            pending_window_in_days: request.pending_window_in_days,
            force_immediate: request.force_immediate,
            confirm_key_id: request.confirm_key_id.clone(),
        };

        // Collected before the request is carried out, so the response
        // describes the deployment the caller is acting on. It is reported,
        // never acted on here: the deletion worker re-runs this check against
        // live configuration before it destroys anything, which is the gate
        // that decides the outcome.
        let impact = current_key_impact(&request.key_id, manager.get_default_key_id().map(String::as_str)).await;

        match manager.delete_key_with_context(kms_request, audit.context()).await {
            Ok(kms_response) => {
                info!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "delete_key",
                    key_id = %kms_response.key_id,
                    state = "completed",
                    "admin kms keys state"
                );
                let response = DeleteKmsKeyResponse {
                    success: true,
                    message: "key deleted successfully".to_string(),
                    key_id: kms_response.key_id,
                    deletion_date: kms_response.deletion_date,
                    impact: Some(impact),
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "delete_key",
                    key_id = %request.key_id,
                    result = "failed",
                    error = %e,
                    "admin kms keys state"
                );
                let status = delete_key_error_status(&e);
                let response = DeleteKmsKeyResponse {
                    success: false,
                    message: format!("Failed to delete key: {e}"),
                    key_id: request.key_id,
                    deletion_date: None,
                    impact: Some(impact),
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((status, Body::from(data)), headers))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CancelKmsKeyDeletionRequest {
    pub key_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CancelKmsKeyDeletionResponse {
    pub success: bool,
    pub message: String,
    pub key_id: String,
    pub key_metadata: Option<KeyMetadata>,
}

/// Cancel KMS key deletion
pub struct CancelKmsKeyDeletionHandler;

#[async_trait::async_trait]
impl Operation for CancelKmsKeyDeletionHandler {
    async fn call(&self, mut req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        // Same ordering as the delete endpoint: the target key is resolved before
        // the gate, the read failure only after it.
        let body = req.input.store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE).await;
        let scoped = body.as_ref().ok().and_then(|body| scoped_key_id(body, &req.uri));
        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate(
            validate_admin_request_with_kms_key(
                &req.headers,
                &cred,
                owner,
                false,
                kms_delete_key_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                scoped.as_deref().unwrap_or_default(),
            )
            .await,
            KmsAuditOperation::CancelKeyDeletion,
            scoped.as_deref(),
        )?;

        let body = body.map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;

        let request: CancelKmsKeyDeletionRequest = if body.is_empty() {
            let query_params = extract_query_params(&req.uri);
            let Some(key_id) = query_params.get("keyId") else {
                let response = CancelKmsKeyDeletionResponse {
                    success: false,
                    message: "missing required parameter: 'keyId'".to_string(),
                    key_id: "".to_string(),
                    key_metadata: None,
                };
                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
                return Ok(S3Response::with_headers((StatusCode::BAD_REQUEST, Body::from(data)), headers));
            };
            CancelKmsKeyDeletionRequest { key_id: key_id.clone() }
        } else {
            serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))?
        };

        let Some(service_manager) = kms_service_manager_from_context() else {
            let response = CancelKmsKeyDeletionResponse {
                success: false,
                message: "kms service manager is not initialized".to_string(),
                key_id: request.key_id,
                key_metadata: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let Some(manager) = service_manager.get_manager().await else {
            let response = CancelKmsKeyDeletionResponse {
                success: false,
                message: "kms service is not running".to_string(),
                key_id: request.key_id,
                key_metadata: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let kms_request = CancelKeyDeletionRequest {
            key_id: request.key_id.clone(),
        };

        match manager.cancel_key_deletion_with_context(kms_request, audit.context()).await {
            Ok(kms_response) => {
                info!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "cancel_delete_key",
                    key_id = %kms_response.key_id,
                    state = "completed",
                    "admin kms keys state"
                );
                let response = CancelKmsKeyDeletionResponse {
                    success: true,
                    message: "key deletion cancelled successfully".to_string(),
                    key_id: kms_response.key_id,
                    key_metadata: Some(kms_response.key_metadata),
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "cancel_delete_key",
                    key_id = %request.key_id,
                    result = "failed",
                    error = %e,
                    "admin kms keys state"
                );
                let response = CancelKmsKeyDeletionResponse {
                    success: false,
                    message: format!("Failed to cancel key deletion: {e}"),
                    key_id: request.key_id,
                    key_metadata: None,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::INTERNAL_SERVER_ERROR, Body::from(data)), headers))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ListKmsKeysResponse {
    pub success: bool,
    pub message: String,
    pub keys: Vec<KeyInfo>,
    pub truncated: bool,
    pub next_marker: Option<String>,
    /// Identifiers present in the key store that this build could not describe.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unreadable_key_ids: Vec<String>,
}

/// List KMS keys
pub struct ListKmsKeysHandler;

#[async_trait::async_trait]
impl Operation for ListKmsKeysHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate(
            validate_admin_request(
                &req.headers,
                &cred,
                owner,
                false,
                kms_list_keys_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
            )
            .await,
            KmsAuditOperation::ListKeys,
            None,
        )?;

        let query_params = extract_query_params(&req.uri);
        let marker = query_params.get("marker").cloned();
        // Validated before the listing runs, so a page size or filter the
        // service cannot apply fails as the input error it is instead of
        // returning the whole key set as if it had been narrowed.
        let parsed =
            parse_list_limit(&query_params).and_then(|limit| key_list_filters(&query_params).map(|filters| (limit, filters)));
        let (limit, filters) = match parsed {
            Ok(parsed) => parsed,
            Err(message) => {
                let response = ListKmsKeysResponse {
                    success: false,
                    message,
                    keys: vec![],
                    truncated: false,
                    next_marker: None,
                    unreadable_key_ids: Vec::new(),
                };
                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
                return Ok(S3Response::with_headers((StatusCode::BAD_REQUEST, Body::from(data)), headers));
            }
        };

        let Some(service_manager) = kms_service_manager_from_context() else {
            let response = ListKmsKeysResponse {
                success: false,
                message: "kms service manager is not initialized".to_string(),
                keys: vec![],
                truncated: false,
                next_marker: None,
                unreadable_key_ids: Vec::new(),
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let Some(manager) = service_manager.get_manager().await else {
            let response = ListKmsKeysResponse {
                success: false,
                message: "kms service is not running".to_string(),
                keys: vec![],
                truncated: false,
                next_marker: None,
                unreadable_key_ids: Vec::new(),
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let kms_request = ListKeysRequest {
            limit,
            marker,
            status_filter: filters.status,
            usage_filter: filters.usage,
        };

        match manager.list_keys_with_context(kms_request, audit.context()).await {
            Ok(kms_response) => {
                info!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "list_keys",
                    state = "completed",
                    key_count = kms_response.keys.len(),
                    "admin kms keys state"
                );
                let response = ListKmsKeysResponse {
                    success: true,
                    message: "keys listed successfully".to_string(),
                    keys: kms_response.keys,
                    truncated: kms_response.truncated,
                    next_marker: kms_response.next_marker,
                    unreadable_key_ids: kms_response.unreadable_key_ids,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "list_keys",
                    result = "failed",
                    error = %e,
                    "admin kms keys state"
                );
                let response = ListKmsKeysResponse {
                    success: false,
                    message: format!("failed to list keys: {e}"),
                    keys: vec![],
                    truncated: false,
                    next_marker: None,
                    unreadable_key_ids: Vec::new(),
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::INTERNAL_SERVER_ERROR, Body::from(data)), headers))
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DescribeKmsKeyResponse {
    pub success: bool,
    pub message: String,
    pub key_metadata: Option<KeyMetadata>,
    /// Configuration that currently points at the key, so the blast radius of
    /// a deletion can be read off without scheduling one first.
    ///
    /// Present only when the request asked for it with `impact=true`; `None`
    /// otherwise, and whenever the key could not be described at all. Absent
    /// therefore means "not collected", never "nothing references this key" —
    /// see [`KeyImpactReport`] for what a collected one does and does not say.
    pub impact: Option<KeyImpactReport>,
}

/// Whether the caller asked for the configuration impact section.
///
/// Off unless asked for. Collecting the section lists every bucket, and this
/// endpoint is polled, so the default read path must not carry that fan-out;
/// the delete path reports unconditionally instead, because that is the
/// request whose consequences the operator cannot otherwise see.
///
/// A value that is neither `true` nor `false` is refused rather than read as
/// "off". Silently downgrading a typo would answer a request for the section
/// with a response that has none, and an absent section must never be
/// mistaken for one that found nothing.
fn wants_key_impact(uri: &hyper::Uri) -> Result<bool, String> {
    match extract_query_params(uri).get("impact") {
        None => Ok(false),
        Some(value) => value
            .parse::<bool>()
            .map_err(|_| format!("invalid value for 'impact': expected 'true' or 'false', got '{value}'")),
    }
}

/// Configuration impact of the described key, collected only when requested.
async fn key_impact_if_requested(requested: bool, key_id: &str, default_key_id: Option<&str>) -> Option<KeyImpactReport> {
    if !requested {
        return None;
    }
    Some(current_key_impact(key_id, default_key_id).await)
}

/// Describe a KMS key
pub struct DescribeKmsKeyHandler;

#[async_trait::async_trait]
impl Operation for DescribeKmsKeyHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "authentication required"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &cred.access_key).await?;

        let audit = KmsAdminAudit::from_request(&req.extensions, &req.headers, &cred);

        audit.gate(
            validate_admin_request_with_kms_key(
                &req.headers,
                &cred,
                owner,
                false,
                kms_describe_key_actions(),
                req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
                params.get("key_id").unwrap_or_default(),
            )
            .await,
            KmsAuditOperation::DescribeKey,
            params.get("key_id"),
        )?;

        let Some(key_id) = params.get("key_id") else {
            let response = DescribeKmsKeyResponse {
                success: false,
                message: "missing required parameter: 'keyId'".to_string(),
                key_metadata: None,
                impact: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
            return Ok(S3Response::with_headers((StatusCode::BAD_REQUEST, Body::from(data)), headers));
        };

        // Validated before the key is looked up, so a malformed opt-in fails as
        // the input error it is instead of riding along on the key's outcome.
        let wants_impact = match wants_key_impact(&req.uri) {
            Ok(wants_impact) => wants_impact,
            Err(message) => {
                let response = DescribeKmsKeyResponse {
                    success: false,
                    message,
                    key_metadata: None,
                    impact: None,
                };
                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
                return Ok(S3Response::with_headers((StatusCode::BAD_REQUEST, Body::from(data)), headers));
            }
        };

        let Some(service_manager) = kms_service_manager_from_context() else {
            let response = DescribeKmsKeyResponse {
                success: false,
                message: "kms service manager is not initialized".to_string(),
                key_metadata: None,
                impact: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let Some(manager) = service_manager.get_manager().await else {
            let response = DescribeKmsKeyResponse {
                success: false,
                message: "kms service is not running".to_string(),
                key_metadata: None,
                impact: None,
            };
            let data =
                serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));
            return Ok(S3Response::with_headers((StatusCode::SERVICE_UNAVAILABLE, Body::from(data)), headers));
        };

        let kms_request = DescribeKeyRequest {
            key_id: key_id.to_string(),
        };

        match manager.describe_key_with_context(kms_request, audit.context()).await {
            Ok(kms_response) => {
                info!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "describe_key",
                    key_id = %key_id,
                    state = "completed",
                    "admin kms keys state"
                );
                // Read-only, and opt-in: the same configuration-layer facts the
                // delete path reports, so the blast radius of a deletion can be
                // inspected without scheduling one first.
                let impact =
                    key_impact_if_requested(wants_impact, key_id, manager.get_default_key_id().map(String::as_str)).await;
                let response = DescribeKmsKeyResponse {
                    success: true,
                    message: "Key described successfully".to_string(),
                    key_metadata: Some(kms_response.key_metadata),
                    impact,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
            }
            Err(e) => {
                error!(
                    event = EVENT_ADMIN_KMS_KEYS_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_KMS_KEYS,
                    action = "describe_key",
                    key_id = %key_id,
                    result = "failed",
                    error = %e,
                    "admin kms keys state"
                );
                let status = match &e {
                    KmsError::KeyNotFound { .. } => StatusCode::NOT_FOUND,
                    KmsError::InvalidOperation { .. } => StatusCode::BAD_REQUEST,
                    // Damaged or missing key material is an integrity fault of an existing
                    // key: it must surface as a server error, never as NOT_FOUND (the key
                    // exists) and never as a retryable backend outage.
                    KmsError::MaterialMissing { .. }
                    | KmsError::MaterialCorrupt { .. }
                    | KmsError::MaterialAuthenticationFailed { .. }
                    | KmsError::UnsupportedFormatVersion { .. } => StatusCode::INTERNAL_SERVER_ERROR,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                };

                let response = DescribeKmsKeyResponse {
                    success: false,
                    message: format!("Failed to describe key: {e}"),
                    key_metadata: None,
                    impact: None,
                };

                let data =
                    serde_json::to_vec(&response).map_err(|e| s3_error!(InternalError, "failed to serialize response: {}", e))?;

                let mut headers = HeaderMap::new();
                headers.insert(CONTENT_TYPE, "application/json".parse().expect("operation should succeed"));

                Ok(S3Response::with_headers((status, Body::from(data)), headers))
            }
        }
    }
}
