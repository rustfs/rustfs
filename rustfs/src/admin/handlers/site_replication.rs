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

use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::utils::{encode_compatible_admin_payload, read_compatible_admin_body};
use crate::auth::{check_key_valid, get_session_token};
use crate::error::ApiError;
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http::header::{CONTENT_TYPE, HOST};
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::{DEFAULT_DELIMITER, MAX_ADMIN_REQUEST_BODY_SIZE};
use rustfs_ecstore::bucket::bucket_target_sys::BucketTargetSys;
use rustfs_ecstore::bucket::metadata::{
    BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_POLICY_CONFIG, BUCKET_QUOTA_CONFIG_FILE, BUCKET_REPLICATION_CONFIG,
    BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG, BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG, OBJECT_LOCK_CONFIG,
};
use rustfs_ecstore::bucket::metadata_sys;
use rustfs_ecstore::bucket::replication::GLOBAL_REPLICATION_STATS;
use rustfs_ecstore::bucket::replication::{ReplicationConfigurationExt, ResyncOpts, get_global_replication_pool};
use rustfs_ecstore::bucket::target::{ARN, BucketTarget, BucketTargetType, BucketTargets, Credentials};
use rustfs_ecstore::bucket::utils::{deserialize, serialize};
use rustfs_ecstore::config::com::{delete_config, read_config, save_config};
use rustfs_ecstore::config::get_global_server_config;
use rustfs_ecstore::error::Error as StorageError;
use rustfs_ecstore::global::{get_global_deployment_id, get_global_endpoints_opt, get_global_region, global_rustfs_port};
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::{BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions, SRBucketDeleteOp};
use rustfs_iam::store::{MappedPolicy, UserType};
use rustfs_iam::sys::{NewServiceAccountOpts, UpdateServiceAccountOpts, get_claims_from_token_with_secret};
use rustfs_iam::{get_global_iam_sys, get_oidc};
use rustfs_madmin::{
    BucketBandwidth, GroupStatus, IDPSettings, InProgressMetric, InQueueMetric, LDAPConfigSettings, LDAPSettings,
    OpenIDProviderSettings, PeerInfo, PeerSite, QStat, ReplProxyMetric, ReplicateAddStatus, ReplicateEditStatus,
    ReplicateRemoveStatus, ResyncBucketStatus, SITE_REPL_API_VERSION, SRBucketInfo, SRBucketMeta, SRBucketStatsSummary,
    SRGroupStatsSummary, SRIAMItem, SRIAMPolicy, SRILMExpiryStatsSummary, SRInfo, SRMetric, SRMetricsSummary, SRPeerJoinReq,
    SRPolicyMapping, SRPolicyStatsSummary, SRRemoveReq, SRResyncOpStatus, SRSiteSummary, SRStateEditReq, SRStateInfo,
    SRStatusInfo, SRUserStatsSummary, SiteReplicationInfo, SyncStatus, WorkerStat,
};
use rustfs_policy::policy::{
    Policy,
    action::{Action, AdminAction},
};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use s3s::dto::{
    BucketVersioningStatus, DeleteMarkerReplication, DeleteMarkerReplicationStatus, DeleteReplication, DeleteReplicationStatus,
    Destination, ExistingObjectReplication, ExistingObjectReplicationStatus, ReplicationConfiguration, ReplicationRule,
    ReplicationRuleStatus, VersioningConfiguration,
};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use url::{Url, form_urlencoded};
use uuid::Uuid;

const SITE_REPLICATION_STATE_PATH: &str = "config/site-replication/state.json";
const SITE_REPL_ADD_SUCCESS: &str = "Requested sites were configured for replication successfully.";
const SITE_REPL_EDIT_SUCCESS: &str = "Requested site was updated successfully.";
const SITE_REPL_REMOVE_SUCCESS: &str = "Requested site(s) were removed from cluster replication successfully.";
const SITE_REPL_RESYNC_START: &str = "start";
const SITE_REPL_RESYNC_CANCEL: &str = "cancel";
const SITE_REPL_MIN_NETPERF_DURATION: Duration = Duration::from_secs(1);
const SITE_REPLICATION_PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SITE_REPLICATION_PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const IDENTITY_LDAP_SUB_SYS: &str = "identity_ldap";
const LEGACY_LDAP_SUB_SYS: &str = "ldapserverconfig";
const SITE_REPLICATOR_SERVICE_ACCOUNT: &str = "site-replicator-0";
const SITE_REPLICATION_PEER_JOIN_PATH: &str = "/rustfs/admin/v3/site-replication/peer/join";
const SITE_REPLICATION_PEER_EDIT_PATH: &str = "/rustfs/admin/v3/site-replication/peer/edit";
const SITE_REPLICATION_PEER_REMOVE_PATH: &str = "/rustfs/admin/v3/site-replication/peer/remove";
static SITE_REPLICATION_PEER_CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SiteReplicationState {
    name: String,
    service_account_access_key: String,
    service_account_secret_key: String,
    service_account_parent: String,
    peers: BTreeMap<String, PeerInfo>,
    updated_at: Option<OffsetDateTime>,
    resync_status: BTreeMap<String, SRResyncOpStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SRPeerJoinResponse {
    peer: PeerInfo,
}

const GO_GOB_SITE_NETPERF_SCHEMA: &[u8] = &[
    0x7d, 0x7f, 0x03, 0x01, 0x01, 0x15, 0x53, 0x69, 0x74, 0x65, 0x4e, 0x65, 0x74, 0x50, 0x65, 0x72, 0x66, 0x4e, 0x6f, 0x64, 0x65,
    0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x01, 0xff, 0x80, 0x00, 0x01, 0x07, 0x01, 0x08, 0x45, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e,
    0x74, 0x01, 0x0c, 0x00, 0x01, 0x02, 0x54, 0x58, 0x01, 0x06, 0x00, 0x01, 0x0f, 0x54, 0x58, 0x54, 0x6f, 0x74, 0x61, 0x6c, 0x44,
    0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x01, 0x04, 0x00, 0x01, 0x02, 0x52, 0x58, 0x01, 0x06, 0x00, 0x01, 0x0f, 0x52, 0x58,
    0x54, 0x6f, 0x74, 0x61, 0x6c, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x01, 0x04, 0x00, 0x01, 0x09, 0x54, 0x6f, 0x74,
    0x61, 0x6c, 0x43, 0x6f, 0x6e, 0x6e, 0x01, 0x06, 0x00, 0x01, 0x05, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x01, 0x0c, 0x00, 0x00, 0x00,
];

#[derive(Debug, Clone)]
struct SiteNetPerfNodeResult {
    endpoint: String,
    tx: u64,
    tx_total_duration_ns: i64,
    rx: u64,
    rx_total_duration_ns: i64,
    total_conn: u64,
    error: String,
}

impl SiteReplicationState {
    fn enabled(&self) -> bool {
        self.peers.len() > 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum SREntityType {
    #[default]
    Unspecified,
    Bucket,
    Policy,
    User,
    Group,
    IlmExpiryRule,
}

#[derive(Debug, Clone, Default)]
struct SRStatusOptions {
    buckets: bool,
    policies: bool,
    users: bool,
    groups: bool,
    metrics: bool,
    peer_state: bool,
    ilm_expiry_rules: bool,
    entity: SREntityType,
    entity_value: String,
}

impl SRStatusOptions {
    fn include_all_defaults(&self) -> bool {
        !(self.buckets
            || self.policies
            || self.users
            || self.groups
            || self.metrics
            || self.peer_state
            || self.ilm_expiry_rules
            || self.entity != SREntityType::Unspecified)
    }
}

pub fn register_site_replication_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    for (method, path, operation) in [
        (Method::PUT, "/v3/site-replication/add", AdminOperation(&SiteReplicationAddHandler {})),
        (
            Method::PUT,
            "/v3/site-replication/remove",
            AdminOperation(&SiteReplicationRemoveHandler {}),
        ),
        (Method::GET, "/v3/site-replication/info", AdminOperation(&SiteReplicationInfoHandler {})),
        (
            Method::GET,
            "/v3/site-replication/metainfo",
            AdminOperation(&SiteReplicationMetaInfoHandler {}),
        ),
        (
            Method::GET,
            "/v3/site-replication/status",
            AdminOperation(&SiteReplicationStatusHandler {}),
        ),
        (
            Method::POST,
            "/v3/site-replication/devnull",
            AdminOperation(&SiteReplicationDevNullHandler {}),
        ),
        (
            Method::POST,
            "/v3/site-replication/netperf",
            AdminOperation(&SiteReplicationNetPerfHandler {}),
        ),
        (Method::PUT, "/v3/site-replication/peer/join", AdminOperation(&SRPeerJoinHandler {})),
        (
            Method::PUT,
            "/v3/site-replication/peer/bucket-ops",
            AdminOperation(&SRPeerBucketOpsHandler {}),
        ),
        (
            Method::PUT,
            "/v3/site-replication/peer/iam-item",
            AdminOperation(&SRPeerReplicateIAMItemHandler {}),
        ),
        (
            Method::PUT,
            "/v3/site-replication/peer/bucket-meta",
            AdminOperation(&SRPeerReplicateBucketItemHandler {}),
        ),
        (
            Method::GET,
            "/v3/site-replication/peer/idp-settings",
            AdminOperation(&SRPeerGetIDPSettingsHandler {}),
        ),
        (Method::PUT, "/v3/site-replication/edit", AdminOperation(&SiteReplicationEditHandler {})),
        (Method::PUT, "/v3/site-replication/peer/edit", AdminOperation(&SRPeerEditHandler {})),
        (Method::PUT, "/v3/site-replication/peer/remove", AdminOperation(&SRPeerRemoveHandler {})),
        (
            Method::PUT,
            "/v3/site-replication/resync/op",
            AdminOperation(&SiteReplicationResyncOpHandler {}),
        ),
        (Method::PUT, "/v3/site-replication/state/edit", AdminOperation(&SRStateEditHandler {})),
    ] {
        r.insert(method, format!("{ADMIN_PREFIX}{path}").as_str(), operation)?;
    }

    Ok(())
}

async fn validate_site_replication_admin_request(
    req: &S3Request<Body>,
    action: AdminAction,
) -> S3Result<rustfs_credentials::Credentials> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "get cred failed"));
    };

    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

    let remote_addr = req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0));
    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(action)], remote_addr).await?;

    Ok(cred)
}

fn json_response<T: Serialize>(value: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(value)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to serialize response: {e}")))?;
    let mut headers = HeaderMap::new();
    headers.insert(s3s::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
}

fn go_gob_site_netperf_response(value: &SiteNetPerfNodeResult) -> S3Response<(StatusCode, Body)> {
    let data = encode_go_gob_site_netperf_node_result(value);
    S3Response::new((StatusCode::OK, Body::from(data)))
}

fn encode_go_gob_site_netperf_node_result(value: &SiteNetPerfNodeResult) -> Vec<u8> {
    let mut data = GO_GOB_SITE_NETPERF_SCHEMA.to_vec();
    let mut payload = Vec::new();
    write_go_gob_int(&mut payload, 64);

    let mut last_field = None;
    encode_go_gob_string_field(&mut payload, &mut last_field, 0, &value.endpoint);
    encode_go_gob_u64_field(&mut payload, &mut last_field, 1, value.tx);
    encode_go_gob_i64_field(&mut payload, &mut last_field, 2, value.tx_total_duration_ns);
    encode_go_gob_u64_field(&mut payload, &mut last_field, 3, value.rx);
    encode_go_gob_i64_field(&mut payload, &mut last_field, 4, value.rx_total_duration_ns);
    encode_go_gob_u64_field(&mut payload, &mut last_field, 5, value.total_conn);
    encode_go_gob_string_field(&mut payload, &mut last_field, 6, &value.error);
    payload.push(0);

    write_go_gob_uint(&mut data, payload.len() as u64);
    data.extend(payload);
    data
}

fn encode_go_gob_string_field(out: &mut Vec<u8>, last_field: &mut Option<usize>, field: usize, value: &str) {
    if value.is_empty() {
        return;
    }
    write_go_gob_field_delta(out, last_field, field);
    write_go_gob_uint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn encode_go_gob_u64_field(out: &mut Vec<u8>, last_field: &mut Option<usize>, field: usize, value: u64) {
    if value == 0 {
        return;
    }
    write_go_gob_field_delta(out, last_field, field);
    write_go_gob_uint(out, value);
}

fn encode_go_gob_i64_field(out: &mut Vec<u8>, last_field: &mut Option<usize>, field: usize, value: i64) {
    if value == 0 {
        return;
    }
    write_go_gob_field_delta(out, last_field, field);
    write_go_gob_int(out, value);
}

fn write_go_gob_field_delta(out: &mut Vec<u8>, last_field: &mut Option<usize>, field: usize) {
    let delta = match *last_field {
        Some(previous) => field - previous,
        None => field + 1,
    };
    write_go_gob_uint(out, delta as u64);
    *last_field = Some(field);
}

fn write_go_gob_int(out: &mut Vec<u8>, value: i64) {
    let encoded = if value < 0 {
        ((!value as u64) << 1) | 1
    } else {
        (value as u64) << 1
    };
    write_go_gob_uint(out, encoded);
}

fn write_go_gob_uint(out: &mut Vec<u8>, value: u64) {
    if value < 128 {
        out.push(value as u8);
        return;
    }

    let bytes = value.to_be_bytes();
    let first_non_zero = bytes.iter().position(|byte| *byte != 0).unwrap_or(bytes.len() - 1);
    let used = &bytes[first_non_zero..];
    out.push((0u8).wrapping_sub(used.len() as u8));
    out.extend_from_slice(used);
}

fn empty_response(status: StatusCode) -> S3Response<(StatusCode, Body)> {
    S3Response::new((status, Body::empty()))
}

async fn read_plain_admin_body(mut input: Body) -> S3Result<Vec<u8>> {
    let body = input
        .store_all_limited(MAX_ADMIN_REQUEST_BODY_SIZE)
        .await
        .map_err(|e| s3_error!(InvalidRequest, "failed to read request body: {}", e))?;
    Ok(body.to_vec())
}

async fn read_site_replication_json<T: DeserializeOwned>(
    req: S3Request<Body>,
    secret_key: &str,
    compat_encrypted: bool,
) -> S3Result<T> {
    let body = if compat_encrypted {
        read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), secret_key).await?
    } else {
        read_plain_admin_body(req.input).await?
    };

    serde_json::from_slice(&body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))
}

async fn load_site_replication_state() -> S3Result<SiteReplicationState> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    match read_config(store, SITE_REPLICATION_STATE_PATH).await {
        Ok(data) => serde_json::from_slice(&data)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("invalid site replication state: {e}"))),
        Err(StorageError::ConfigNotFound) => Ok(SiteReplicationState::default()),
        Err(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to load site replication state: {err}"),
        )),
    }
}

async fn save_site_replication_state(state: &SiteReplicationState) -> S3Result<()> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    let data = serde_json::to_vec(state)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize state failed: {e}")))?;
    save_config(store, SITE_REPLICATION_STATE_PATH, data)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("save state failed: {e}")))?;
    Ok(())
}

async fn clear_site_replication_state() -> S3Result<()> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    match delete_config(store, SITE_REPLICATION_STATE_PATH).await {
        Ok(()) | Err(StorageError::ConfigNotFound) => Ok(()),
        Err(err) => Err(S3Error::with_message(S3ErrorCode::InternalError, format!("clear state failed: {err}"))),
    }
}

async fn persist_site_replication_state(state: &SiteReplicationState) -> S3Result<()> {
    if state.peers.len() <= 1 {
        clear_site_replication_state().await
    } else {
        save_site_replication_state(state).await
    }
}

fn site_replication_peer_client() -> &'static reqwest::Client {
    SITE_REPLICATION_PEER_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(SITE_REPLICATION_PEER_REQUEST_TIMEOUT)
            .connect_timeout(SITE_REPLICATION_PEER_CONNECT_TIMEOUT)
            .pool_idle_timeout(Some(Duration::from_secs(60)))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
}

fn query_pairs(uri: &Uri) -> HashMap<String, String> {
    uri.query()
        .map(|query| {
            form_urlencoded::parse(query.as_bytes())
                .into_owned()
                .collect::<HashMap<String, String>>()
        })
        .unwrap_or_default()
}

fn query_flag(uri: &Uri, key: &str) -> bool {
    query_pairs(uri).get(key).is_some_and(|value| value == "true")
}

fn sr_entity_type(value: &str) -> SREntityType {
    match value {
        "bucket" => SREntityType::Bucket,
        "policy" => SREntityType::Policy,
        "user" => SREntityType::User,
        "group" => SREntityType::Group,
        "ilm-expiry-rule" => SREntityType::IlmExpiryRule,
        _ => SREntityType::Unspecified,
    }
}

fn sr_status_options(uri: &Uri) -> SRStatusOptions {
    let pairs = query_pairs(uri);
    SRStatusOptions {
        buckets: pairs.get("buckets").is_some_and(|value| value == "true"),
        policies: pairs.get("policies").is_some_and(|value| value == "true"),
        users: pairs.get("users").is_some_and(|value| value == "true"),
        groups: pairs.get("groups").is_some_and(|value| value == "true"),
        metrics: pairs.get("metrics").is_some_and(|value| value == "true"),
        peer_state: pairs.get("peer-state").is_some_and(|value| value == "true"),
        ilm_expiry_rules: pairs.get("ilm-expiry-rules").is_some_and(|value| value == "true"),
        entity: pairs
            .get("entity")
            .map(String::as_str)
            .map(sr_entity_type)
            .unwrap_or(SREntityType::Unspecified),
        entity_value: pairs.get("entityvalue").cloned().unwrap_or_default(),
    }
}

fn sr_add_replicate_ilm_expiry(uri: &Uri) -> bool {
    query_flag(uri, "replicateILMExpiry")
}

fn sr_edit_ilm_expiry_override(uri: &Uri) -> Option<bool> {
    if query_flag(uri, "enableILMExpiryReplication") {
        Some(true)
    } else if query_flag(uri, "disableILMExpiryReplication") {
        Some(false)
    } else {
        None
    }
}

fn hash_client_secret(secret: Option<&str>) -> String {
    let Some(secret) = secret.filter(|secret| !secret.is_empty()) else {
        return String::new();
    };

    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    URL_SAFE_NO_PAD.encode(hasher.finalize())
}

fn config_enabled(value: Option<String>) -> bool {
    matches!(value.as_deref(), Some("on" | "true" | "enabled"))
}

fn ldap_settings_from_kvs(kvs: &rustfs_ecstore::config::KVS) -> (LDAPSettings, LDAPConfigSettings) {
    let enabled = config_enabled(kvs.lookup("enable"));
    let settings = LDAPSettings {
        is_ldap_enabled: enabled,
        ldap_user_dn_search_base: kvs.get("user_dn_search_base_dn"),
        ldap_user_dn_search_filter: kvs.get("user_dn_search_filter"),
        ldap_group_search_base: kvs.get("group_search_base_dn"),
        ldap_group_search_filter: kvs.get("group_search_filter"),
    };

    let mut ldap_configs = LDAPConfigSettings {
        enabled,
        ..Default::default()
    };

    if !settings.ldap_user_dn_search_base.is_empty()
        || !settings.ldap_user_dn_search_filter.is_empty()
        || !settings.ldap_group_search_base.is_empty()
        || !settings.ldap_group_search_filter.is_empty()
    {
        ldap_configs.configs.insert(
            "default".to_string(),
            rustfs_madmin::LDAPProviderSettings {
                user_dn_search_base: settings.ldap_user_dn_search_base.clone(),
                user_dn_search_filter: settings.ldap_user_dn_search_filter.clone(),
                group_search_base: settings.ldap_group_search_base.clone(),
                group_search_filter: settings.ldap_group_search_filter.clone(),
            },
        );
    }

    (settings, ldap_configs)
}

fn load_ldap_idp_settings() -> (LDAPSettings, LDAPConfigSettings) {
    let Some(config) = get_global_server_config() else {
        return (LDAPSettings::default(), LDAPConfigSettings::default());
    };

    let ldap_kvs = config
        .get_value(IDENTITY_LDAP_SUB_SYS, DEFAULT_DELIMITER)
        .or_else(|| config.get_value(LEGACY_LDAP_SUB_SYS, DEFAULT_DELIMITER));

    ldap_kvs
        .as_ref()
        .map(ldap_settings_from_kvs)
        .unwrap_or_else(|| (LDAPSettings::default(), LDAPConfigSettings::default()))
}

fn request_endpoint(uri: &Uri, headers: &HeaderMap) -> String {
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .unwrap_or("http");

    let host = headers
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            get_global_endpoints_opt().and_then(|endpoints| {
                endpoints
                    .as_ref()
                    .iter()
                    .flat_map(|pool| pool.endpoints.as_ref().iter())
                    .find(|endpoint| endpoint.is_local)
                    .map(|endpoint| endpoint.host_port())
            })
        })
        .unwrap_or_else(|| format!("127.0.0.1:{}", global_rustfs_port()));

    if uri.scheme_str().is_some() {
        return format!("{scheme}://{host}");
    }

    format!("{scheme}://{host}")
}

fn current_local_runtime_endpoint() -> String {
    request_endpoint(&Uri::from_static("/"), &HeaderMap::new())
}

fn infer_site_name(endpoint: &str) -> String {
    endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or_default()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_string()
}

fn deployment_id_for_endpoint(endpoint: &str) -> String {
    let mut hasher = DefaultHasher::new();
    endpoint.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

fn qstat(count: i64, bytes: i64) -> QStat {
    QStat {
        count: count as f64,
        bytes: bytes as f64,
    }
}

fn non_negative_u64(value: i64) -> u64 {
    value.max(0) as u64
}

fn current_local_peer(req: &S3Request<Body>, state: &SiteReplicationState) -> PeerInfo {
    let endpoint = request_endpoint(&req.uri, &req.headers);
    let deployment_id = get_global_deployment_id().unwrap_or_else(|| deployment_id_for_endpoint(&endpoint));
    let stored_peer = state.peers.get(&deployment_id);

    PeerInfo {
        endpoint: endpoint.clone(),
        name: if state.name.is_empty() {
            stored_peer
                .map(|peer| peer.name.clone())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| infer_site_name(&endpoint))
        } else {
            state.name.clone()
        },
        deployment_id,
        sync_state: stored_peer.map(|peer| peer.sync_state.clone()).unwrap_or(SyncStatus::Unknown),
        default_bandwidth: stored_peer.map(|peer| peer.default_bandwidth.clone()).unwrap_or_default(),
        replicate_ilm_expiry: stored_peer.is_some_and(|peer| peer.replicate_ilm_expiry),
        object_naming_mode: stored_peer.map(|peer| peer.object_naming_mode.clone()).unwrap_or_default(),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    }
}

fn current_local_runtime_peer(state: &SiteReplicationState) -> PeerInfo {
    let endpoint = current_local_runtime_endpoint();
    let deployment_id = get_global_deployment_id().unwrap_or_else(|| deployment_id_for_endpoint(&endpoint));
    let stored_peer = state.peers.get(&deployment_id);

    PeerInfo {
        endpoint: endpoint.clone(),
        name: if state.name.is_empty() {
            stored_peer
                .map(|peer| peer.name.clone())
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| infer_site_name(&endpoint))
        } else {
            state.name.clone()
        },
        deployment_id,
        sync_state: stored_peer.map(|peer| peer.sync_state.clone()).unwrap_or(SyncStatus::Unknown),
        default_bandwidth: stored_peer.map(|peer| peer.default_bandwidth.clone()).unwrap_or_default(),
        replicate_ilm_expiry: stored_peer.is_some_and(|peer| peer.replicate_ilm_expiry),
        object_naming_mode: stored_peer.map(|peer| peer.object_naming_mode.clone()).unwrap_or_default(),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    }
}

fn canonical_endpoint(endpoint: &str) -> String {
    let trimmed = endpoint.trim().trim_end_matches('/');
    let candidate = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    };

    Url::parse(&candidate)
        .ok()
        .map(|url| {
            let scheme = url.scheme().to_ascii_lowercase();
            let host = url.host_str().unwrap_or_default().to_ascii_lowercase();
            let port = url.port_or_known_default();
            match port {
                Some(port) => format!("{scheme}://{host}:{port}"),
                None => format!("{scheme}://{host}"),
            }
        })
        .unwrap_or_else(|| trimmed.to_ascii_lowercase())
}

fn same_endpoint(left: &str, right: &str) -> bool {
    canonical_endpoint(left) == canonical_endpoint(right)
}

fn existing_peer_for_endpoint(state: &SiteReplicationState, endpoint: &str) -> Option<PeerInfo> {
    state
        .peers
        .values()
        .find(|peer| same_endpoint(&peer.endpoint, endpoint))
        .cloned()
}

fn peer_deployment_id_for_endpoint(state: &SiteReplicationState, endpoint: &str) -> Option<String> {
    existing_peer_for_endpoint(state, endpoint)
        .map(|peer| peer.deployment_id)
        .filter(|deployment_id| !deployment_id.is_empty())
}

fn normalize_peer_info(mut peer: PeerInfo) -> PeerInfo {
    if peer.deployment_id.is_empty() {
        peer.deployment_id = deployment_id_for_endpoint(&peer.endpoint);
    }
    if peer.name.is_empty() {
        peer.name = infer_site_name(&peer.endpoint);
    }
    if peer.api_version.is_none() {
        peer.api_version = Some(SITE_REPL_API_VERSION.to_string());
    }
    peer
}

fn normalize_peer_site(site: PeerSite, replicate_ilm_expiry: bool) -> PeerInfo {
    normalize_peer_info(PeerInfo {
        endpoint: site.endpoint,
        name: site.name,
        deployment_id: String::new(),
        sync_state: SyncStatus::Unknown,
        default_bandwidth: BucketBandwidth::default(),
        replicate_ilm_expiry,
        object_naming_mode: String::new(),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    })
}

fn build_join_peers(
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    sites: Vec<PeerSite>,
    replicate_ilm_expiry: bool,
) -> BTreeMap<String, PeerInfo> {
    let mut peers = BTreeMap::new();
    let mut seen_endpoints = HashSet::new();

    let mut normalized_local = local_peer.clone();
    normalized_local.replicate_ilm_expiry = replicate_ilm_expiry;
    normalized_local = normalize_peer_info(normalized_local);
    seen_endpoints.insert(canonical_endpoint(&normalized_local.endpoint));
    peers.insert(normalized_local.deployment_id.clone(), normalized_local);

    for site in sites {
        let endpoint_key = canonical_endpoint(&site.endpoint);
        if !seen_endpoints.insert(endpoint_key) {
            continue;
        }

        let mut peer = existing_peer_for_endpoint(state, &site.endpoint)
            .unwrap_or_else(|| normalize_peer_site(site.clone(), replicate_ilm_expiry));
        peer.endpoint = site.endpoint;
        if !site.name.is_empty() {
            peer.name = site.name;
        }
        peer.replicate_ilm_expiry |= replicate_ilm_expiry;
        peer = normalize_peer_info(peer);
        peers.insert(peer.deployment_id.clone(), peer);
    }

    peers
}

fn normalize_join_peers_for_local(local_peer: &PeerInfo, peers: BTreeMap<String, PeerInfo>) -> BTreeMap<String, PeerInfo> {
    let mut normalized = BTreeMap::new();

    for (_, incoming_peer) in peers {
        let mut peer = normalize_peer_info(incoming_peer);
        if same_endpoint(&peer.endpoint, &local_peer.endpoint) {
            peer.deployment_id = local_peer.deployment_id.clone();
            if peer.name.is_empty() {
                peer.name = local_peer.name.clone();
            }
        }
        normalized.insert(peer.deployment_id.clone(), peer);
    }

    if !normalized.contains_key(&local_peer.deployment_id) {
        normalized.insert(local_peer.deployment_id.clone(), local_peer.clone());
    }

    normalized
}

fn reconcile_peer_with_actual_identity(mut state: SiteReplicationState, actual_peer: PeerInfo) -> SiteReplicationState {
    let actual_peer = normalize_peer_info(actual_peer);
    state
        .peers
        .retain(|_, peer| !same_endpoint(&peer.endpoint, &actual_peer.endpoint));
    state.peers.insert(actual_peer.deployment_id.clone(), actual_peer);
    state
}

async fn ensure_site_replicator_service_account(parent_user: &str, state: &SiteReplicationState) -> S3Result<(String, String)> {
    let Some(iam_sys) = get_global_iam_sys() else {
        return Err(s3_error!(InvalidRequest, "iam not init"));
    };

    let access_key = SITE_REPLICATOR_SERVICE_ACCOUNT.to_string();
    let secret_key =
        if state.service_account_access_key == SITE_REPLICATOR_SERVICE_ACCOUNT && !state.service_account_secret_key.is_empty() {
            state.service_account_secret_key.clone()
        } else {
            rustfs_credentials::gen_secret_key(40)
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("generate secret key failed: {e}")))?
        };

    if iam_sys.get_service_account(&access_key).await.is_ok() {
        iam_sys
            .update_service_account(
                &access_key,
                UpdateServiceAccountOpts {
                    session_policy: None,
                    secret_key: Some(secret_key.clone()),
                    name: None,
                    description: None,
                    expiration: None,
                    status: None,
                },
            )
            .await
            .map_err(ApiError::from)?;
    } else {
        iam_sys
            .new_service_account(
                parent_user,
                None,
                NewServiceAccountOpts {
                    session_policy: None,
                    access_key: access_key.clone(),
                    secret_key: secret_key.clone(),
                    name: None,
                    description: None,
                    expiration: None,
                    allow_site_replicator_account: true,
                    claims: None,
                },
            )
            .await
            .map_err(ApiError::from)?;
    }

    Ok((access_key, secret_key))
}

async fn send_peer_admin_request<T: Serialize>(
    endpoint: &str,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<Vec<u8>> {
    let base = endpoint.trim_end_matches('/');
    let url = format!("{base}{path}");
    let uri = url
        .parse::<Uri>()
        .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid peer endpoint: {e}")))?;
    let authority = uri
        .authority()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "peer endpoint missing authority".to_string()))?
        .to_string();
    let payload = serde_json::to_vec(body)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize peer request failed: {e}")))?;
    let (payload, content_type) = encode_compatible_admin_payload(path, secret_key, payload)?;

    let signed = sign_v4(
        http::Request::builder()
            .method(Method::PUT)
            .uri(uri)
            .header(HOST, authority)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
            .header(CONTENT_TYPE, content_type)
            .body(Body::empty())
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("build peer request failed: {e}")))?,
        payload.len() as i64,
        access_key,
        secret_key,
        "",
        get_global_region()
            .map(|region| region.to_string())
            .as_deref()
            .unwrap_or("us-east-1"),
    );

    let mut req = site_replication_peer_client().request(reqwest::Method::PUT, &url);
    for (name, value) in signed.headers() {
        req = req.header(name, value);
    }

    let response = req
        .body(payload)
        .send()
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("peer request failed: {e}")))?;

    let status = response.status();
    let body = response
        .bytes()
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("read peer response failed: {e}")))?;

    if !status.is_success() {
        let detail = String::from_utf8_lossy(&body).into_owned();
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("peer request to {url} failed with {status}: {detail}"),
        ));
    }

    Ok(body.to_vec())
}

async fn runtime_site_replication_targets() -> S3Result<Option<(SiteReplicationState, PeerInfo)>> {
    let state = load_site_replication_state().await?;
    if !state.enabled() || state.service_account_access_key.is_empty() || state.service_account_secret_key.is_empty() {
        return Ok(None);
    }

    Ok(Some((state.clone(), current_local_runtime_peer(&state))))
}

async fn broadcast_site_replication_json<T: Serialize>(path: &str, body: &T) -> S3Result<()> {
    let Some((state, local_peer)) = runtime_site_replication_targets().await? else {
        return Ok(());
    };

    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }

        send_peer_admin_request(
            &peer.endpoint,
            path,
            &state.service_account_access_key,
            &state.service_account_secret_key,
            body,
        )
        .await?;
    }

    Ok(())
}

pub async fn site_replication_make_bucket_hook(bucket: &str, lock_enabled: bool) -> S3Result<()> {
    let Some((state, local_peer)) = runtime_site_replication_targets().await? else {
        return Ok(());
    };

    ensure_site_replication_bucket_targets(bucket, &state, &local_peer, None).await?;
    ensure_site_replication_bucket_replication_config(bucket, &state, &local_peer).await?;

    let created_at = new_object_layer_fn()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?
        .get_bucket_info(bucket, &BucketOptions::default())
        .await
        .map_err(ApiError::from)?
        .created
        .unwrap_or_else(OffsetDateTime::now_utc)
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_default();

    let path = {
        let mut query = form_urlencoded::Serializer::new(String::new());
        query.append_pair("bucket", bucket);
        query.append_pair("operation", "make-with-versioning");
        query.append_pair("createdAt", &created_at);
        if lock_enabled {
            query.append_pair("lockEnabled", "true");
        }
        format!("/rustfs/admin/v3/site-replication/peer/bucket-ops?{}", query.finish())
    };
    broadcast_site_replication_json(&path, &serde_json::json!({})).await?;

    let configure_path = format!(
        "/rustfs/admin/v3/site-replication/peer/bucket-ops?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("bucket", bucket)
            .append_pair("operation", "configure-replication")
            .finish()
    );
    broadcast_site_replication_json(&configure_path, &serde_json::json!({})).await
}

pub async fn site_replication_delete_bucket_hook(bucket: &str, force_delete: bool) -> S3Result<()> {
    let operation = if force_delete {
        "force-delete-bucket"
    } else {
        "delete-bucket"
    };
    let path = format!(
        "/rustfs/admin/v3/site-replication/peer/bucket-ops?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("bucket", bucket)
            .append_pair("operation", operation)
            .finish()
    );
    broadcast_site_replication_json(&path, &serde_json::json!({})).await
}

pub async fn site_replication_bucket_meta_hook(item: SRBucketMeta) -> S3Result<()> {
    broadcast_site_replication_json("/rustfs/admin/v3/site-replication/peer/bucket-meta", &item).await
}

pub async fn site_replication_iam_change_hook(item: SRIAMItem) -> S3Result<()> {
    broadcast_site_replication_json("/rustfs/admin/v3/site-replication/peer/iam-item", &item).await
}

fn raw_config_to_string(raw: &[u8]) -> Option<String> {
    if raw.is_empty() {
        return None;
    }
    String::from_utf8(raw.to_vec()).ok()
}

fn maybe_time(value: OffsetDateTime) -> Option<OffsetDateTime> {
    (value != OffsetDateTime::UNIX_EPOCH).then_some(value)
}

async fn build_sr_info(state: &SiteReplicationState, local_peer: &PeerInfo) -> S3Result<SRInfo> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    let mut info = SRInfo {
        enabled: state.enabled(),
        name: local_peer.name.clone(),
        deployment_id: local_peer.deployment_id.clone(),
        state: SRStateInfo {
            name: local_peer.name.clone(),
            peers: state.peers.clone(),
            updated_at: state.updated_at,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        },
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    };

    let buckets = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;
    for bucket in buckets {
        let metadata = metadata_sys::get(&bucket.name).await.ok();
        let mut entry = SRBucketInfo {
            bucket: bucket.name.clone(),
            created_at: bucket.created,
            location: get_global_region().map(|region| region.to_string()).unwrap_or_default(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        };

        if let Some(metadata) = metadata {
            entry.policy = raw_config_to_string(&metadata.policy_config_json).and_then(|raw| serde_json::from_str(&raw).ok());
            entry.versioning = raw_config_to_string(&metadata.versioning_config_xml);
            entry.tags = raw_config_to_string(&metadata.tagging_config_xml);
            entry.object_lock_config = raw_config_to_string(&metadata.object_lock_config_xml);
            entry.sse_config = raw_config_to_string(&metadata.encryption_config_xml);
            entry.replication_config = raw_config_to_string(&metadata.replication_config_xml);
            entry.quota_config = raw_config_to_string(&metadata.quota_config_json);
            entry.expiry_lc_config = raw_config_to_string(&metadata.lifecycle_config_xml);
            entry.cors_config = raw_config_to_string(&metadata.cors_config_xml);
            entry.policy_updated_at = maybe_time(metadata.policy_config_updated_at);
            entry.tag_config_updated_at = maybe_time(metadata.tagging_config_updated_at);
            entry.object_lock_config_updated_at = maybe_time(metadata.object_lock_config_updated_at);
            entry.sse_config_updated_at = maybe_time(metadata.encryption_config_updated_at);
            entry.versioning_config_updated_at = maybe_time(metadata.versioning_config_updated_at);
            entry.replication_config_updated_at = maybe_time(metadata.replication_config_updated_at);
            entry.quota_config_updated_at = maybe_time(metadata.quota_config_updated_at);
            entry.expiry_lc_config_updated_at = maybe_time(metadata.lifecycle_config_updated_at);
            entry.cors_config_updated_at = maybe_time(metadata.cors_config_updated_at);
        }

        info.buckets.insert(bucket.name, entry);
    }

    if let Some(iam_sys) = get_global_iam_sys() {
        for (name, policy_doc) in iam_sys.list_policy_docs("").await.map_err(ApiError::from)? {
            info.policies.insert(
                name,
                SRIAMPolicy {
                    policy: serde_json::to_value(policy_doc.policy).ok(),
                    updated_at: policy_doc.update_date,
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                },
            );
        }

        let users = iam_sys.list_users().await.map_err(ApiError::from)?;
        for (name, user) in users {
            info.user_info_map.insert(name, user);
        }

        let groups = iam_sys.list_groups_load().await.map_err(ApiError::from)?;
        for group in groups {
            let desc = iam_sys.get_group_description(&group).await.map_err(ApiError::from)?;
            info.group_desc_map.insert(group.clone(), desc);
        }

        let mut user_policies = HashMap::<String, MappedPolicy>::new();
        iam_sys
            .load_mapped_policies(UserType::Reg, false, &mut user_policies)
            .await
            .map_err(ApiError::from)?;
        for (name, mapping) in user_policies {
            info.user_policies
                .insert(name.clone(), mapped_policy_to_sr_mapping(name, false, UserType::Reg, mapping));
        }

        let mut group_policies = HashMap::<String, MappedPolicy>::new();
        iam_sys
            .load_mapped_policies(UserType::None, true, &mut group_policies)
            .await
            .map_err(ApiError::from)?;
        for (name, mapping) in group_policies {
            info.group_policies
                .insert(name.clone(), mapped_policy_to_sr_mapping(name, true, UserType::None, mapping));
        }
    }

    for (name, bucket_info) in &info.buckets {
        if let Some(raw) = bucket_info
            .replication_config
            .as_ref()
            .and_then(|value| serde_json::from_str::<Value>(value).ok())
        {
            info.replication_cfg.insert(name.clone(), raw);
        }
    }

    Ok(info)
}

fn mapped_policy_to_sr_mapping(name: String, is_group: bool, user_type: UserType, mapping: MappedPolicy) -> SRPolicyMapping {
    SRPolicyMapping {
        user_or_group: name,
        user_type: user_type.to_u64(),
        is_group,
        policy: mapping.policies,
        updated_at: Some(mapping.update_at),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    }
}

fn filter_sr_info(mut info: SRInfo, opts: &SRStatusOptions) -> SRInfo {
    if opts.include_all_defaults() {
        return info;
    }

    let include_buckets =
        opts.buckets || opts.metrics || matches!(opts.entity, SREntityType::Bucket | SREntityType::IlmExpiryRule);
    if !include_buckets {
        info.buckets.clear();
        info.replication_cfg.clear();
    } else if opts.entity == SREntityType::Bucket && !opts.entity_value.is_empty() {
        info.buckets.retain(|name, _| name == &opts.entity_value);
        info.replication_cfg.retain(|name, _| name == &opts.entity_value);
    }

    let include_policies = opts.policies || opts.entity == SREntityType::Policy;
    if !include_policies {
        info.policies.clear();
    } else if opts.entity == SREntityType::Policy && !opts.entity_value.is_empty() {
        info.policies.retain(|name, _| name == &opts.entity_value);
    }

    let include_users = opts.users || opts.entity == SREntityType::User;
    if !include_users {
        info.user_info_map.clear();
        info.user_policies.clear();
    } else if opts.entity == SREntityType::User && !opts.entity_value.is_empty() {
        info.user_info_map.retain(|name, _| name == &opts.entity_value);
        info.user_policies.retain(|name, _| name == &opts.entity_value);
    }

    let include_groups = opts.groups || opts.entity == SREntityType::Group;
    if !include_groups {
        info.group_desc_map.clear();
        info.group_policies.clear();
    } else if opts.entity == SREntityType::Group && !opts.entity_value.is_empty() {
        info.group_desc_map.retain(|name, _| name == &opts.entity_value);
        info.group_policies.retain(|name, _| name == &opts.entity_value);
    }

    let include_ilm_expiry = opts.ilm_expiry_rules || opts.entity == SREntityType::IlmExpiryRule;
    if !include_ilm_expiry {
        info.ilm_expiry_rules.clear();
    } else if opts.entity == SREntityType::IlmExpiryRule && !opts.entity_value.is_empty() {
        info.ilm_expiry_rules.retain(|name, _| name == &opts.entity_value);
    }

    info
}

fn build_site_summary(info: &SRInfo) -> SRSiteSummary {
    let replicated_buckets = info.buckets.len();
    let replicated_tags = info.buckets.values().filter(|bucket| bucket.tags.is_some()).count();
    let replicated_bucket_policies = info.buckets.values().filter(|bucket| bucket.policy.is_some()).count();
    let replicated_lock_config = info
        .buckets
        .values()
        .filter(|bucket| bucket.object_lock_config.is_some())
        .count();
    let replicated_sse_config = info.buckets.values().filter(|bucket| bucket.sse_config.is_some()).count();
    let replicated_versioning_config = info.buckets.values().filter(|bucket| bucket.versioning.is_some()).count();
    let replicated_quota_config = info.buckets.values().filter(|bucket| bucket.quota_config.is_some()).count();
    let replicated_cors_config = info.buckets.values().filter(|bucket| bucket.cors_config.is_some()).count();

    SRSiteSummary {
        replicated_buckets,
        replicated_tags,
        replicated_bucket_policies,
        replicated_iam_policies: info.policies.len(),
        replicated_users: info.user_info_map.len(),
        replicated_groups: info.group_desc_map.len(),
        replicated_lock_config,
        replicated_sse_config,
        replicated_versioning_config,
        replicated_quota_config,
        replicated_user_policy_mappings: info.user_policies.len(),
        replicated_group_policy_mappings: info.group_policies.len(),
        replicated_ilm_expiry_rules: info.ilm_expiry_rules.len(),
        replicated_cors_config,
        total_buckets_count: info.buckets.len(),
        total_tags_count: replicated_tags,
        total_bucket_policies_count: replicated_bucket_policies,
        total_iam_policies_count: info.policies.len(),
        total_lock_config_count: replicated_lock_config,
        total_sse_config_count: replicated_sse_config,
        total_versioning_config_count: replicated_versioning_config,
        total_quota_config_count: replicated_quota_config,
        total_users_count: info.user_info_map.len(),
        total_groups_count: info.group_desc_map.len(),
        total_user_policy_mapping_count: info.user_policies.len(),
        total_group_policy_mapping_count: info.group_policies.len(),
        total_ilm_expiry_rules_count: info.ilm_expiry_rules.len(),
        total_cors_config_count: replicated_cors_config,
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    }
}

async fn build_metrics_summary(local_peer: &PeerInfo) -> SRMetricsSummary {
    let Some(stats) = GLOBAL_REPLICATION_STATS.get() else {
        return SRMetricsSummary::default();
    };

    let node = stats.get_sr_metrics_for_node().await;
    let mut metrics = BTreeMap::new();
    metrics.insert(
        local_peer.deployment_id.clone(),
        SRMetric {
            deployment_id: local_peer.deployment_id.clone(),
            endpoint: local_peer.endpoint.clone(),
            online: true,
            replicated_size: node.replica_size,
            replicated_count: node.replica_count,
            last_online: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        },
    );

    SRMetricsSummary {
        active_workers: WorkerStat {
            curr: node.active_workers.curr,
            avg: node.active_workers.avg,
            max: node.active_workers.max,
        },
        replica_size: node.replica_size,
        replica_count: node.replica_count,
        queued: InQueueMetric {
            curr: qstat(node.queued.curr.count, node.queued.curr.bytes),
            avg: qstat(node.queued.avg.count, node.queued.avg.bytes),
            max: qstat(node.queued.max.count, node.queued.max.bytes),
        },
        in_progress: InProgressMetric::default(),
        proxied: ReplProxyMetric {
            get_total: non_negative_u64(node.proxied.get_total),
            head_total: non_negative_u64(node.proxied.head_total),
            get_failed_total: non_negative_u64(node.proxied.get_failed),
            head_failed_total: non_negative_u64(node.proxied.head_failed),
            put_tag_total: non_negative_u64(node.proxied.put_total),
            put_tag_failed_total: non_negative_u64(node.proxied.put_failed),
            ..Default::default()
        },
        metrics,
        uptime: node.uptime,
        ..Default::default()
    }
}

async fn build_status_info(state: &SiteReplicationState, local_peer: &PeerInfo, uri: &Uri) -> S3Result<SRStatusInfo> {
    let opts = sr_status_options(uri);
    let info = filter_sr_info(build_sr_info(state, local_peer).await?, &opts);
    let metrics_requested = opts.metrics || opts.include_all_defaults() || opts.entity == SREntityType::Bucket;

    let mut status = SRStatusInfo {
        enabled: state.enabled(),
        max_buckets: info.buckets.len(),
        max_users: info.user_info_map.len(),
        max_groups: info.group_desc_map.len(),
        max_policies: info.policies.len(),
        max_ilm_expiry_rules: info.ilm_expiry_rules.len(),
        sites: state.peers.clone(),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    };

    for deployment_id in state.peers.keys() {
        let summary = if deployment_id == &local_peer.deployment_id {
            build_site_summary(&info)
        } else {
            SRSiteSummary {
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            }
        };
        status.stats_summary.insert(deployment_id.clone(), summary);

        if deployment_id != &local_peer.deployment_id {
            continue;
        }

        if opts.include_all_defaults() || opts.buckets || opts.entity == SREntityType::Bucket {
            for (bucket_name, bucket_info) in &info.buckets {
                if opts.entity == SREntityType::Bucket && !opts.entity_value.is_empty() && bucket_name != &opts.entity_value {
                    continue;
                }
                status.bucket_stats.entry(bucket_name.clone()).or_default().insert(
                    deployment_id.clone(),
                    SRBucketStatsSummary {
                        deployment_id: deployment_id.clone(),
                        has_bucket: true,
                        has_tags_set: bucket_info.tags.is_some(),
                        has_object_lock_config_set: bucket_info.object_lock_config.is_some(),
                        has_policy_set: bucket_info.policy.is_some(),
                        has_sse_cfg_set: bucket_info.sse_config.is_some(),
                        has_replication_cfg: bucket_info.replication_config.is_some(),
                        has_quota_cfg_set: bucket_info.quota_config.is_some(),
                        has_cors_cfg_set: bucket_info.cors_config.is_some(),
                        api_version: Some(SITE_REPL_API_VERSION.to_string()),
                        ..Default::default()
                    },
                );
            }
        }

        if opts.include_all_defaults() || opts.policies || opts.entity == SREntityType::Policy {
            for name in info.policies.keys() {
                if opts.entity == SREntityType::Policy && !opts.entity_value.is_empty() && name != &opts.entity_value {
                    continue;
                }
                status.policy_stats.entry(name.clone()).or_default().insert(
                    deployment_id.clone(),
                    SRPolicyStatsSummary {
                        deployment_id: deployment_id.clone(),
                        has_policy: true,
                        api_version: Some(SITE_REPL_API_VERSION.to_string()),
                        ..Default::default()
                    },
                );
            }
        }

        if opts.include_all_defaults() || opts.users || opts.entity == SREntityType::User {
            for name in info.user_info_map.keys() {
                if opts.entity == SREntityType::User && !opts.entity_value.is_empty() && name != &opts.entity_value {
                    continue;
                }
                status.user_stats.entry(name.clone()).or_default().insert(
                    deployment_id.clone(),
                    SRUserStatsSummary {
                        deployment_id: deployment_id.clone(),
                        has_user: true,
                        has_policy_mapping: info.user_policies.contains_key(name),
                        api_version: Some(SITE_REPL_API_VERSION.to_string()),
                        ..Default::default()
                    },
                );
            }
        }

        if opts.include_all_defaults() || opts.groups || opts.entity == SREntityType::Group {
            for name in info.group_desc_map.keys() {
                if opts.entity == SREntityType::Group && !opts.entity_value.is_empty() && name != &opts.entity_value {
                    continue;
                }
                status.group_stats.entry(name.clone()).or_default().insert(
                    deployment_id.clone(),
                    SRGroupStatsSummary {
                        deployment_id: deployment_id.clone(),
                        has_group: true,
                        has_policy_mapping: info.group_policies.contains_key(name),
                        api_version: Some(SITE_REPL_API_VERSION.to_string()),
                        ..Default::default()
                    },
                );
            }
        }

        if opts.include_all_defaults() || opts.ilm_expiry_rules || opts.entity == SREntityType::IlmExpiryRule {
            for name in info.ilm_expiry_rules.keys() {
                if opts.entity == SREntityType::IlmExpiryRule && !opts.entity_value.is_empty() && name != &opts.entity_value {
                    continue;
                }
                status.ilm_expiry_stats.entry(name.clone()).or_default().insert(
                    deployment_id.clone(),
                    SRILMExpiryStatsSummary {
                        deployment_id: deployment_id.clone(),
                        has_ilm_expiry_rules: true,
                        api_version: Some(SITE_REPL_API_VERSION.to_string()),
                        ..Default::default()
                    },
                );
            }
        }
    }

    if metrics_requested {
        status.metrics = build_metrics_summary(local_peer).await;
    }

    if opts.peer_state {
        for (deployment_id, peer) in &state.peers {
            status.peer_states.insert(
                deployment_id.clone(),
                SRStateInfo {
                    name: peer.name.clone(),
                    peers: state.peers.clone(),
                    updated_at: state.updated_at,
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                },
            );
        }
    }

    Ok(status)
}

fn merge_add_sites(
    mut state: SiteReplicationState,
    local_peer: PeerInfo,
    sites: Vec<PeerSite>,
    service_account_access_key: String,
    service_account_secret_key: String,
    service_account_parent: String,
    replicate_ilm_expiry: bool,
) -> SiteReplicationState {
    state.name = local_peer.name.clone();
    state.service_account_access_key = service_account_access_key;
    state.service_account_secret_key = service_account_secret_key;
    state.service_account_parent = service_account_parent;
    state.updated_at = Some(OffsetDateTime::now_utc());
    state.peers = build_join_peers(&state, &local_peer, sites, replicate_ilm_expiry);
    state
}

fn update_peer(mut state: SiteReplicationState, incoming: PeerInfo, ilm_expiry_override: Option<bool>) -> SiteReplicationState {
    let mut peer = normalize_peer_info(incoming);
    if let Some(enabled) = ilm_expiry_override {
        peer.replicate_ilm_expiry = enabled;
    }
    state.updated_at = Some(OffsetDateTime::now_utc());
    state.peers.insert(peer.deployment_id.clone(), peer);
    state
}

fn sync_state_name_for_local_peer(
    mut state: SiteReplicationState,
    local_peer: &PeerInfo,
    incoming: &PeerInfo,
) -> SiteReplicationState {
    if same_endpoint(&incoming.endpoint, &local_peer.endpoint) && !incoming.name.is_empty() {
        state.name = incoming.name.clone();
    }
    state
}

fn edit_state(mut state: SiteReplicationState, incoming: PeerInfo, ilm_expiry_override: Option<bool>) -> SiteReplicationState {
    if let Some(enabled) = ilm_expiry_override {
        for peer in state.peers.values_mut() {
            peer.replicate_ilm_expiry = enabled;
        }
    }

    if !incoming.deployment_id.is_empty() || !incoming.endpoint.is_empty() || !incoming.name.is_empty() {
        state = update_peer(state, incoming, ilm_expiry_override);
    } else {
        state.updated_at = Some(OffsetDateTime::now_utc());
    }

    state
}

fn remove_sites(mut state: SiteReplicationState, req: SRRemoveReq) -> SiteReplicationState {
    if req.remove_all {
        state.peers.clear();
        state.resync_status.clear();
        state.updated_at = Some(OffsetDateTime::now_utc());
        return state;
    }

    let names: Vec<String> = req.site_names.into_iter().collect();
    state.peers.retain(|_, peer| !names.iter().any(|name| name == &peer.name));
    state.updated_at = Some(OffsetDateTime::now_utc());
    state
}

fn resync_status_for_state(
    state: &mut SiteReplicationState,
    op_type: &str,
    peer: &PeerInfo,
    bucket_names: Vec<String>,
) -> SRResyncOpStatus {
    let status = SRResyncOpStatus {
        op_type: op_type.to_string(),
        resync_id: Uuid::new_v4().to_string(),
        status: "success".to_string(),
        buckets: bucket_names
            .into_iter()
            .map(|bucket| ResyncBucketStatus {
                bucket,
                status: if op_type == SITE_REPL_RESYNC_CANCEL {
                    "canceled".to_string()
                } else {
                    "started".to_string()
                },
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    state.resync_status.insert(peer.deployment_id.clone(), status.clone());
    status
}

fn bucket_target_endpoint(target: &BucketTarget) -> String {
    let scheme = if target.secure { "https" } else { "http" };
    canonical_endpoint(&format!("{scheme}://{}", target.endpoint))
}

fn bucket_target_matches_peer(target: &BucketTarget, peer: &PeerInfo) -> bool {
    (!target.deployment_id.is_empty() && target.deployment_id == peer.deployment_id)
        || bucket_target_endpoint(target) == canonical_endpoint(&peer.endpoint)
}

fn site_replication_target_arns_by_peer(config: Option<&s3s::dto::ReplicationConfiguration>) -> HashMap<String, String> {
    let mut arns_by_peer = HashMap::new();
    let Some(config) = config else {
        return arns_by_peer;
    };

    let mut configured_arns = Vec::new();
    if !config.role.trim().is_empty() {
        configured_arns.push(config.role.clone());
    }
    for rule in &config.rules {
        let arn = rule.destination.bucket.trim();
        if !arn.is_empty() {
            configured_arns.push(arn.to_string());
        }
    }

    for arn in configured_arns {
        if let Ok(parsed) = arn.parse::<ARN>()
            && parsed.arn_type == BucketTargetType::ReplicationService
            && !parsed.id.is_empty()
        {
            arns_by_peer.entry(parsed.id).or_insert(arn);
        }
    }

    arns_by_peer
}

fn site_replication_bucket_target_for_peer(
    bucket: &str,
    state: &SiteReplicationState,
    peer: &PeerInfo,
    arn_override: Option<String>,
) -> Option<BucketTarget> {
    if state.service_account_access_key.is_empty() || state.service_account_secret_key.is_empty() {
        return None;
    }

    let parsed = Url::parse(&peer.endpoint)
        .ok()
        .or_else(|| Url::parse(&format!("http://{}", peer.endpoint.trim())).ok())?;
    let host = parsed.host_str()?;
    let port = parsed.port_or_known_default()?;
    let arn = arn_override.unwrap_or_else(|| {
        ARN::new(
            BucketTargetType::ReplicationService,
            peer.deployment_id.clone(),
            String::new(),
            bucket.to_string(),
        )
        .to_string()
    });

    Some(BucketTarget {
        source_bucket: bucket.to_string(),
        endpoint: format!("{host}:{port}"),
        credentials: Some(Credentials {
            access_key: state.service_account_access_key.clone(),
            secret_key: state.service_account_secret_key.clone(),
            session_token: None,
            expiration: None,
        }),
        target_bucket: bucket.to_string(),
        secure: parsed.scheme().eq_ignore_ascii_case("https"),
        arn,
        target_type: BucketTargetType::ReplicationService,
        deployment_id: peer.deployment_id.clone(),
        ..Default::default()
    })
}

fn reconcile_site_replication_bucket_targets(
    existing: BucketTargets,
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    config: Option<&s3s::dto::ReplicationConfiguration>,
) -> BucketTargets {
    if !state.enabled() || state.service_account_access_key.is_empty() || state.service_account_secret_key.is_empty() {
        return existing;
    }

    let configured_arns = site_replication_target_arns_by_peer(config);
    let mut targets = existing.targets;

    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }

        let Some(mut target) =
            site_replication_bucket_target_for_peer(bucket, state, peer, configured_arns.get(&peer.deployment_id).cloned())
        else {
            continue;
        };

        if let Some(index) = targets.iter().position(|existing| {
            existing.target_type == BucketTargetType::ReplicationService
                && (bucket_target_matches_peer(existing, peer) || existing.arn == target.arn)
        }) {
            let existing = targets[index].clone();
            target.path = existing.path;
            target.region = existing.region;
            target.bandwidth_limit = existing.bandwidth_limit;
            target.replication_sync = existing.replication_sync;
            target.storage_class = existing.storage_class;
            target.health_check_duration = existing.health_check_duration;
            target.disable_proxy = existing.disable_proxy;
            target.reset_before_date = existing.reset_before_date;
            target.reset_id = existing.reset_id;
            target.total_downtime = existing.total_downtime;
            target.last_online = existing.last_online;
            target.online = existing.online;
            target.latency = existing.latency;
            target.edge = existing.edge;
            target.edge_sync_before_expiry = existing.edge_sync_before_expiry;
            target.offline_count = existing.offline_count;
            targets[index] = target;
        } else {
            targets.push(target);
        }
    }

    BucketTargets { targets }
}

fn build_site_replication_rule(arn: &str, priority: i32, rule_id: &str) -> ReplicationRule {
    ReplicationRule {
        delete_marker_replication: Some(DeleteMarkerReplication {
            status: Some(DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED)),
        }),
        delete_replication: Some(DeleteReplication {
            status: DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED),
        }),
        destination: Destination {
            bucket: arn.to_string(),
            ..Default::default()
        },
        existing_object_replication: Some(ExistingObjectReplication {
            status: ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED),
        }),
        filter: None,
        id: Some(rule_id.to_string()),
        prefix: None,
        priority: Some(priority),
        source_selection_criteria: None,
        status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
    }
}

fn build_site_replication_config(
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
) -> Option<ReplicationConfiguration> {
    let mut rules = Vec::new();
    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }

        let Some(target) = site_replication_bucket_target_for_peer(bucket, state, peer, None) else {
            continue;
        };
        rules.push(build_site_replication_rule(
            &target.arn,
            (rules.len() + 1) as i32,
            &format!("site-repl-{}", peer.deployment_id),
        ));
    }

    if rules.is_empty() {
        None
    } else {
        Some(ReplicationConfiguration {
            role: String::new(),
            rules,
        })
    }
}

async fn ensure_site_replication_bucket_targets(
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    config: Option<&s3s::dto::ReplicationConfiguration>,
) -> S3Result<()> {
    let existing = match metadata_sys::list_bucket_targets(bucket).await {
        Ok(targets) => targets,
        Err(StorageError::ConfigNotFound) => BucketTargets::default(),
        Err(err) => return Err(ApiError::from(err).into()),
    };

    let updated = reconcile_site_replication_bucket_targets(existing, bucket, state, local_peer, config);
    if updated.targets.is_empty() {
        return Ok(());
    }

    let json_targets = serde_json::to_vec(&updated)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize bucket targets failed: {e}")))?;
    metadata_sys::update(bucket, BUCKET_TARGETS_FILE, json_targets)
        .await
        .map_err(ApiError::from)?;
    BucketTargetSys::get().update_all_targets(bucket, Some(&updated)).await;

    Ok(())
}

async fn ensure_site_replication_bucket_replication_config(
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
) -> S3Result<()> {
    match metadata_sys::get_replication_config(bucket).await {
        Ok(_) => return Ok(()),
        Err(StorageError::ConfigNotFound) => {}
        Err(err) => return Err(ApiError::from(err).into()),
    }

    let Some(config) = build_site_replication_config(bucket, state, local_peer) else {
        return Ok(());
    };

    let data = serialize(&config)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize replication failed: {e}")))?;
    metadata_sys::update(bucket, BUCKET_REPLICATION_CONFIG, data)
        .await
        .map_err(ApiError::from)?;

    Ok(())
}

pub async fn site_replication_peer_deployment_id_for_endpoint(endpoint: &str) -> Option<String> {
    let state = load_site_replication_state().await.ok()?;
    peer_deployment_id_for_endpoint(&state, endpoint)
}

async fn start_site_bucket_resync(bucket: &str, peer: &PeerInfo, resync_id: &str) -> ResyncBucketStatus {
    let mut bucket_status = ResyncBucketStatus {
        bucket: bucket.to_string(),
        status: "started".to_string(),
        ..Default::default()
    };

    let (config, _) = match metadata_sys::get_replication_config(bucket).await {
        Ok(config) => config,
        Err(err) => {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = err.to_string();
            return bucket_status;
        }
    };

    let mut targets = match metadata_sys::list_bucket_targets(bucket).await {
        Ok(targets) => targets,
        Err(err) => {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = err.to_string();
            return bucket_status;
        }
    };

    let reset_before = Some(OffsetDateTime::now_utc());
    let target_arn = {
        let Some(target) = targets.targets.iter_mut().find(|target| {
            target.target_type == BucketTargetType::ReplicationService && bucket_target_matches_peer(target, peer)
        }) else {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = format!("no valid remote target found for peer {}", peer.deployment_id);
            return bucket_status;
        };

        let (has_arn, existing_object_enabled) = config.has_existing_object_replication(&target.arn);
        if !has_arn || !existing_object_enabled {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = "existing object replication is not enabled for the peer target".to_string();
            return bucket_status;
        }

        target.reset_id = resync_id.to_string();
        target.reset_before_date = reset_before;
        target.arn.clone()
    };

    let json_targets = match serde_json::to_vec(&targets) {
        Ok(json_targets) => json_targets,
        Err(err) => {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = err.to_string();
            return bucket_status;
        }
    };

    if let Err(err) = metadata_sys::update(bucket, BUCKET_TARGETS_FILE, json_targets).await {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = err.to_string();
        return bucket_status;
    }
    BucketTargetSys::get().update_all_targets(bucket, Some(&targets)).await;

    let Some(pool) = get_global_replication_pool() else {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = "replication pool is not initialized".to_string();
        return bucket_status;
    };

    if let Err(err) = pool
        .start_bucket_resync(ResyncOpts {
            bucket: bucket.to_string(),
            arn: target_arn,
            resync_id: resync_id.to_string(),
            resync_before: reset_before,
        })
        .await
    {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = err.to_string();
    }

    bucket_status
}

async fn cancel_site_bucket_resync(bucket: &str, peer: &PeerInfo, resync_id: &str) -> ResyncBucketStatus {
    let mut bucket_status = ResyncBucketStatus {
        bucket: bucket.to_string(),
        status: "canceled".to_string(),
        ..Default::default()
    };

    let mut targets = match metadata_sys::list_bucket_targets(bucket).await {
        Ok(targets) => targets,
        Err(err) => {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = err.to_string();
            return bucket_status;
        }
    };

    let Some(target) = targets.targets.iter_mut().find(|target| {
        target.target_type == BucketTargetType::ReplicationService
            && bucket_target_matches_peer(target, peer)
            && target.reset_id == resync_id
    }) else {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = format!("no in-progress resync target found for peer {}", peer.deployment_id);
        return bucket_status;
    };

    target.reset_id.clear();
    target.reset_before_date = None;
    let target_arn = target.arn.clone();

    let json_targets = match serde_json::to_vec(&targets) {
        Ok(json_targets) => json_targets,
        Err(err) => {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = err.to_string();
            return bucket_status;
        }
    };

    if let Err(err) = metadata_sys::update(bucket, BUCKET_TARGETS_FILE, json_targets).await {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = err.to_string();
        return bucket_status;
    }
    BucketTargetSys::get().update_all_targets(bucket, Some(&targets)).await;

    let Some(pool) = get_global_replication_pool() else {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = "replication pool is not initialized".to_string();
        return bucket_status;
    };

    if let Err(err) = pool
        .cancel_bucket_resync(ResyncOpts {
            bucket: bucket.to_string(),
            arn: target_arn,
            resync_id: resync_id.to_string(),
            resync_before: None,
        })
        .await
    {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = err.to_string();
    }

    bucket_status
}

fn apply_state_edit_req(mut state: SiteReplicationState, body: SRStateEditReq) -> SiteReplicationState {
    let incoming_updated_at = body.updated_at.unwrap_or_else(OffsetDateTime::now_utc);
    if state.updated_at.is_some_and(|current| incoming_updated_at <= current) {
        return state;
    }

    for (deployment_id, mut peer) in body.peers {
        if peer.deployment_id.is_empty() {
            peer.deployment_id = deployment_id.clone();
        }
        if let Some(current_peer) = state.peers.get_mut(&deployment_id) {
            current_peer.replicate_ilm_expiry = peer.replicate_ilm_expiry;
        } else {
            state.peers.insert(deployment_id, normalize_peer_info(peer));
        }
    }

    state.updated_at = Some(incoming_updated_at);
    state
}

fn bucket_versioning_xml() -> S3Result<Vec<u8>> {
    let config = VersioningConfiguration {
        status: Some(BucketVersioningStatus::from_static(BucketVersioningStatus::ENABLED)),
        ..Default::default()
    };
    serialize(&config).map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize versioning failed: {e}")))
}

async fn apply_bucket_meta_item(item: SRBucketMeta) -> S3Result<()> {
    let Some(store) = new_object_layer_fn() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    store
        .get_bucket_info(&item.bucket, &BucketOptions::default())
        .await
        .map_err(ApiError::from)?;

    let config_file = match item.r#type.as_str() {
        "policy" => BUCKET_POLICY_CONFIG,
        "tags" => BUCKET_TAGGING_CONFIG,
        "version-config" => BUCKET_VERSIONING_CONFIG,
        "object-lock-config" => OBJECT_LOCK_CONFIG,
        "sse-config" => BUCKET_SSECONFIG,
        "replication-config" => BUCKET_REPLICATION_CONFIG,
        "quota-config" => BUCKET_QUOTA_CONFIG_FILE,
        "lc-config" => BUCKET_LIFECYCLE_CONFIG,
        "cors-config" => BUCKET_CORS_CONFIG,
        _ => {
            return Err(s3_error!(
                NotImplemented,
                "site replication bucket metadata type `{}` is not supported",
                item.r#type
            ));
        }
    };

    let replication_config = if item.r#type == "replication-config" {
        item.replication_config
            .as_ref()
            .map(|raw| deserialize::<s3s::dto::ReplicationConfiguration>(raw.as_bytes()))
            .transpose()
            .map_err(|e| s3_error!(InvalidRequest, "invalid replication config: {e}"))?
    } else {
        None
    };

    let data = match item.r#type.as_str() {
        "policy" => item
            .policy
            .map(|policy| serde_json::to_vec(&policy))
            .transpose()
            .map_err(|e| s3_error!(InvalidRequest, "invalid bucket policy: {}", e))?,
        "quota-config" => item
            .quota
            .map(|quota| serde_json::to_vec(&quota))
            .transpose()
            .map_err(|e| s3_error!(InvalidRequest, "invalid bucket quota: {}", e))?,
        "tags" => item.tags.map(String::into_bytes),
        "version-config" => item.versioning.map(String::into_bytes),
        "object-lock-config" => item.object_lock_config.map(String::into_bytes),
        "sse-config" => item.sse_config.map(String::into_bytes),
        "replication-config" => item.replication_config.map(String::into_bytes),
        "lc-config" => item.expiry_lc_config.map(String::into_bytes),
        "cors-config" => item
            .cors
            .map(|raw| BASE64_STANDARD.decode(raw.as_bytes()).unwrap_or_else(|_| raw.into_bytes())),
        _ => unreachable!(),
    };

    if let Some(data) = data {
        metadata_sys::update(&item.bucket, config_file, data)
            .await
            .map_err(ApiError::from)?;
    } else {
        metadata_sys::delete(&item.bucket, config_file)
            .await
            .map_err(ApiError::from)?;
    }

    if item.r#type == "replication-config"
        && let Some((state, local_peer)) = runtime_site_replication_targets().await?
    {
        ensure_site_replication_bucket_targets(&item.bucket, &state, &local_peer, replication_config.as_ref()).await?;
    }
    Ok(())
}

fn group_info_requires_upsert(update: &rustfs_madmin::GroupAddRemove) -> bool {
    !update.is_remove
}

async fn apply_iam_item(item: SRIAMItem) -> S3Result<()> {
    let Some(iam_sys) = get_global_iam_sys() else {
        return Err(s3_error!(InvalidRequest, "iam not init"));
    };

    match item.r#type.as_str() {
        "policy" => {
            if let Some(policy) = item.policy {
                let policy: Policy =
                    serde_json::from_value(policy).map_err(|e| s3_error!(InvalidRequest, "invalid policy body: {}", e))?;
                iam_sys.set_policy(&item.name, policy).await.map_err(ApiError::from)?;
            } else {
                iam_sys.delete_policy(&item.name, true).await.map_err(ApiError::from)?;
            }
            Ok(())
        }
        "policy-mapping" => {
            let Some(mapping) = item.policy_mapping else {
                return Err(s3_error!(InvalidRequest, "policyMapping is required"));
            };
            let user_type = UserType::from_u64(mapping.user_type).ok_or_else(|| s3_error!(InvalidRequest, "invalid userType"))?;
            iam_sys
                .policy_db_set(&mapping.user_or_group, user_type, mapping.is_group, &mapping.policy)
                .await
                .map_err(ApiError::from)?;
            Ok(())
        }
        "group-info" => {
            let Some(group_info) = item.group_info else {
                return Err(s3_error!(InvalidRequest, "groupInfo is required"));
            };
            let update = group_info.update_req;
            if !group_info_requires_upsert(&update) {
                iam_sys
                    .remove_users_from_group(&update.group, update.members)
                    .await
                    .map_err(ApiError::from)?;
                return Ok(());
            }

            iam_sys
                .add_users_to_group(&update.group, update.members)
                .await
                .map_err(ApiError::from)?;
            iam_sys
                .set_group_status(&update.group, matches!(update.status, GroupStatus::Enabled))
                .await
                .map_err(ApiError::from)?;
            Ok(())
        }
        "sts-credential" => {
            let Some(sts_credential) = item.sts_credential else {
                return Err(s3_error!(InvalidRequest, "stsCredential is required"));
            };
            let Some(secret) = rustfs_iam::manager::get_token_signing_key() else {
                return Err(s3_error!(InvalidRequest, "token signing key not initialized"));
            };
            let claims = get_claims_from_token_with_secret(&sts_credential.session_token, &secret)
                .map_err(|e| s3_error!(InvalidRequest, "invalid STS session token: {e}"))?;
            let expiration = claims
                .get("exp")
                .and_then(claims_unix_timestamp)
                .map(OffsetDateTime::from_unix_timestamp)
                .transpose()
                .map_err(|e| s3_error!(InvalidRequest, "invalid STS expiry: {e}"))?;
            let cred = rustfs_credentials::Credentials {
                access_key: sts_credential.access_key.clone(),
                secret_key: sts_credential.secret_key.clone(),
                session_token: sts_credential.session_token.clone(),
                expiration,
                status: "on".to_string(),
                parent_user: sts_credential.parent_user.clone(),
                claims: Some(claims),
                ..Default::default()
            };
            iam_sys
                .set_temp_user(
                    &sts_credential.access_key,
                    &cred,
                    (!sts_credential.parent_policy_mapping.is_empty()).then_some(sts_credential.parent_policy_mapping.as_str()),
                )
                .await
                .map_err(ApiError::from)?;
            Ok(())
        }
        "iam-user" => {
            let Some(user) = item.iam_user else {
                return Err(s3_error!(InvalidRequest, "iamUser is required"));
            };
            if user.is_delete_req {
                iam_sys.delete_user(&user.access_key, true).await.map_err(ApiError::from)?;
            } else {
                let Some(user_req) = user.user_req else {
                    return Err(s3_error!(InvalidRequest, "userReq is required"));
                };
                iam_sys
                    .create_user(&user.access_key, &user_req)
                    .await
                    .map_err(ApiError::from)?;
            }
            Ok(())
        }
        "service-account" => {
            let Some(change) = item.svc_acc_change else {
                return Err(s3_error!(InvalidRequest, "serviceAccountChange is required"));
            };
            if let Some(create) = change.create {
                let session_policy = create.session_policy.as_str().and_then(|raw| serde_json::from_str(raw).ok());
                if iam_sys.get_service_account(&create.access_key).await.is_ok() {
                    iam_sys
                        .update_service_account(
                            &create.access_key,
                            UpdateServiceAccountOpts {
                                session_policy,
                                secret_key: Some(create.secret_key),
                                name: (!create.name.is_empty()).then_some(create.name),
                                description: (!create.description.is_empty()).then_some(create.description),
                                expiration: create.expiration,
                                status: (!create.status.is_empty()).then_some(create.status),
                            },
                        )
                        .await
                        .map_err(ApiError::from)?;
                } else {
                    iam_sys
                        .new_service_account(
                            &create.parent,
                            Some(create.groups),
                            NewServiceAccountOpts {
                                session_policy,
                                access_key: create.access_key,
                                secret_key: create.secret_key,
                                name: (!create.name.is_empty()).then_some(create.name),
                                description: (!create.description.is_empty()).then_some(create.description),
                                expiration: create.expiration,
                                allow_site_replicator_account: true,
                                claims: Some(create.claims),
                            },
                        )
                        .await
                        .map_err(ApiError::from)?;
                }
                return Ok(());
            }

            if let Some(update) = change.update {
                let session_policy = update.session_policy.as_str().and_then(|raw| serde_json::from_str(raw).ok());
                iam_sys
                    .update_service_account(
                        &update.access_key,
                        UpdateServiceAccountOpts {
                            session_policy,
                            secret_key: (!update.secret_key.is_empty()).then_some(update.secret_key),
                            name: (!update.name.is_empty()).then_some(update.name),
                            description: (!update.description.is_empty()).then_some(update.description),
                            expiration: update.expiration,
                            status: (!update.status.is_empty()).then_some(update.status),
                        },
                    )
                    .await
                    .map_err(ApiError::from)?;
                return Ok(());
            }

            if let Some(delete) = change.delete {
                iam_sys
                    .delete_service_account(&delete.access_key, true)
                    .await
                    .map_err(ApiError::from)?;
                return Ok(());
            }

            Err(s3_error!(InvalidRequest, "serviceAccountChange is empty"))
        }
        _ => Err(s3_error!(
            NotImplemented,
            "site replication IAM item type `{}` is not supported",
            item.r#type
        )),
    }
}

fn claims_unix_timestamp(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number.as_i64(),
        Value::String(raw) => raw.parse().ok(),
        _ => None,
    }
}

pub struct SiteReplicationAddHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationAddHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationAddAction).await?;
        let replicate_ilm_expiry = sr_add_replicate_ilm_expiry(&req.uri);
        let current_state = load_site_replication_state().await?;
        let local_peer = current_local_peer(&req, &current_state);
        let sites: Vec<PeerSite> = read_site_replication_json(req, &cred.secret_key, true).await?;
        let (service_account_access_key, service_account_secret_key) =
            ensure_site_replicator_service_account(&cred.access_key, &current_state).await?;
        let mut state = merge_add_sites(
            current_state,
            local_peer.clone(),
            sites.clone(),
            service_account_access_key.clone(),
            service_account_secret_key.clone(),
            cred.access_key.clone(),
            replicate_ilm_expiry,
        );
        let join_req = SRPeerJoinReq {
            svc_acct_access_key: service_account_access_key,
            svc_acct_secret_key: service_account_secret_key,
            svc_acct_parent: String::new(),
            peers: state.peers.clone(),
            updated_at: state.updated_at,
        };

        let mut joined_endpoints = HashSet::new();
        for site in &sites {
            let endpoint_key = canonical_endpoint(&site.endpoint);
            if same_endpoint(&site.endpoint, &local_peer.endpoint) || !joined_endpoints.insert(endpoint_key) {
                continue;
            }

            let mut peer_join_req = join_req.clone();
            peer_join_req.svc_acct_parent = site.access_key.clone();
            let body = send_peer_admin_request(
                &site.endpoint,
                SITE_REPLICATION_PEER_JOIN_PATH,
                &site.access_key,
                &site.secret_key,
                &peer_join_req,
            )
            .await?;

            let join_response: SRPeerJoinResponse = serde_json::from_slice(&body).map_err(|e| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("parse peer join response from {} failed: {e}", site.endpoint),
                )
            })?;
            state = reconcile_peer_with_actual_identity(state, join_response.peer);
        }

        persist_site_replication_state(&state).await?;
        json_response(&ReplicateAddStatus {
            success: true,
            status: SITE_REPL_ADD_SUCCESS.to_string(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
    }
}

pub struct SiteReplicationRemoveHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationRemoveHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationRemoveAction).await?;
        let current_state = load_site_replication_state().await?;
        let local_peer = current_local_peer(&req, &current_state);
        let remove_req: SRRemoveReq = read_site_replication_json(req, "", false).await?;

        if !current_state.service_account_access_key.is_empty() && !current_state.service_account_secret_key.is_empty() {
            for peer in current_state.peers.values() {
                if same_endpoint(&peer.endpoint, &local_peer.endpoint) {
                    continue;
                }
                send_peer_admin_request(
                    &peer.endpoint,
                    SITE_REPLICATION_PEER_REMOVE_PATH,
                    &current_state.service_account_access_key,
                    &current_state.service_account_secret_key,
                    &SRRemoveReq {
                        requesting_dep_id: local_peer.deployment_id.clone(),
                        site_names: remove_req.site_names.clone(),
                        remove_all: remove_req.remove_all,
                    },
                )
                .await?;
            }
        }

        let state = remove_sites(current_state, remove_req);
        persist_site_replication_state(&state).await?;
        json_response(&ReplicateRemoveStatus {
            status: SITE_REPL_REMOVE_SUCCESS.to_string(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
    }
}

pub struct SiteReplicationInfoHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationInfoAction).await?;
        let state = load_site_replication_state().await?;
        let local_peer = current_local_peer(&req, &state);
        let info = SiteReplicationInfo {
            enabled: state.enabled(),
            name: local_peer.name,
            sites: state.peers.values().cloned().collect(),
            service_account_access_key: state.service_account_access_key,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        };
        json_response(&info)
    }
}

pub struct SiteReplicationMetaInfoHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationMetaInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationInfoAction).await?;
        let state = load_site_replication_state().await?;
        let local_peer = current_local_peer(&req, &state);
        let opts = sr_status_options(&req.uri);
        let info = filter_sr_info(build_sr_info(&state, &local_peer).await?, &opts);
        json_response(&info)
    }
}

pub struct SiteReplicationStatusHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationInfoAction).await?;
        let state = load_site_replication_state().await?;
        let local_peer = current_local_peer(&req, &state);
        let status = build_status_info(&state, &local_peer, &req.uri).await?;
        json_response(&status)
    }
}

pub struct SiteReplicationDevNullHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationDevNullHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationInfoAction).await?;
        let _ = read_plain_admin_body(req.input).await?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}

pub struct SiteReplicationNetPerfHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationNetPerfHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationInfoAction).await?;
        let duration = query_pairs(&req.uri)
            .get("duration")
            .and_then(|value| rustfs_madmin::utils::parse_duration(value).ok())
            .unwrap_or(SITE_REPL_MIN_NETPERF_DURATION)
            .max(SITE_REPL_MIN_NETPERF_DURATION);

        let endpoint = request_endpoint(&req.uri, &req.headers);
        let started_at = Instant::now();
        let body = read_plain_admin_body(req.input).await?;
        let elapsed = started_at.elapsed().max(duration);

        Ok(go_gob_site_netperf_response(&SiteNetPerfNodeResult {
            endpoint,
            tx: body.len() as u64,
            tx_total_duration_ns: elapsed.as_nanos() as i64,
            rx: body.len() as u64,
            rx_total_duration_ns: elapsed.as_nanos() as i64,
            total_conn: 1,
            error: String::new(),
        }))
    }
}

pub struct SRPeerJoinHandler {}

#[async_trait::async_trait]
impl Operation for SRPeerJoinHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationAddAction).await?;
        let mut state = load_site_replication_state().await?;
        let local_peer = current_local_peer(&req, &state);
        let join_req: SRPeerJoinReq = read_site_replication_json(req, &cred.secret_key, true).await?;

        if !join_req.svc_acct_access_key.is_empty() && !join_req.svc_acct_secret_key.is_empty() {
            let Some(iam_sys) = get_global_iam_sys() else {
                return Err(s3_error!(InvalidRequest, "iam not init"));
            };

            if iam_sys.get_service_account(&join_req.svc_acct_access_key).await.is_ok() {
                iam_sys
                    .update_service_account(
                        &join_req.svc_acct_access_key,
                        UpdateServiceAccountOpts {
                            session_policy: None,
                            secret_key: Some(join_req.svc_acct_secret_key.clone()),
                            name: None,
                            description: None,
                            expiration: None,
                            status: None,
                        },
                    )
                    .await
                    .map_err(ApiError::from)?;
            } else {
                iam_sys
                    .new_service_account(
                        &join_req.svc_acct_parent,
                        None,
                        NewServiceAccountOpts {
                            session_policy: None,
                            access_key: join_req.svc_acct_access_key.clone(),
                            secret_key: join_req.svc_acct_secret_key.clone(),
                            name: None,
                            description: None,
                            expiration: None,
                            allow_site_replicator_account: join_req.svc_acct_access_key == SITE_REPLICATOR_SERVICE_ACCOUNT,
                            claims: None,
                        },
                    )
                    .await
                    .map_err(ApiError::from)?;
            }
        }

        state.service_account_access_key = join_req.svc_acct_access_key;
        state.service_account_secret_key = join_req.svc_acct_secret_key;
        state.service_account_parent = join_req.svc_acct_parent;
        state.updated_at = join_req.updated_at.or_else(|| Some(OffsetDateTime::now_utc()));
        state.peers = normalize_join_peers_for_local(&local_peer, join_req.peers);
        state.name = state
            .peers
            .get(&local_peer.deployment_id)
            .map(|peer| peer.name.clone())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| local_peer.name.clone());
        persist_site_replication_state(&state).await?;
        json_response(&SRPeerJoinResponse {
            peer: state.peers.get(&local_peer.deployment_id).cloned().unwrap_or(local_peer),
        })
    }
}

pub struct SRPeerBucketOpsHandler {}

#[async_trait::async_trait]
impl Operation for SRPeerBucketOpsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        let queries = query_pairs(&req.uri);
        let bucket = queries
            .get("bucket")
            .filter(|bucket| !bucket.is_empty())
            .cloned()
            .ok_or_else(|| s3_error!(InvalidRequest, "bucket is required"))?;
        let operation = queries
            .get("operation")
            .filter(|value| !value.is_empty())
            .cloned()
            .ok_or_else(|| s3_error!(InvalidRequest, "operation is required"))?;

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        match operation.as_str() {
            "make-with-versioning" => {
                let created_at = queries
                    .get("createdAt")
                    .and_then(|value| OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok());
                let lock_enabled = queries.get("lockEnabled").is_some_and(|value| value == "true");
                store
                    .make_bucket(
                        &bucket,
                        &MakeBucketOptions {
                            versioning_enabled: true,
                            lock_enabled,
                            created_at,
                            force_create: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(ApiError::from)?;
                metadata_sys::update(&bucket, BUCKET_VERSIONING_CONFIG, bucket_versioning_xml()?)
                    .await
                    .map_err(ApiError::from)?;
            }
            "configure-replication" => {
                store
                    .get_bucket_info(&bucket, &BucketOptions::default())
                    .await
                    .map_err(ApiError::from)?;
                if let Some((state, local_peer)) = runtime_site_replication_targets().await? {
                    let replication_config = metadata_sys::get_replication_config(&bucket)
                        .await
                        .ok()
                        .map(|(config, _)| config);
                    ensure_site_replication_bucket_targets(&bucket, &state, &local_peer, replication_config.as_ref()).await?;
                    ensure_site_replication_bucket_replication_config(&bucket, &state, &local_peer).await?;
                }
            }
            "delete-bucket" => {
                store
                    .delete_bucket(
                        &bucket,
                        &DeleteBucketOptions {
                            force: false,
                            srdelete_op: SRBucketDeleteOp::MarkDelete,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(ApiError::from)?;
            }
            "force-delete-bucket" => {
                store
                    .delete_bucket(
                        &bucket,
                        &DeleteBucketOptions {
                            force: true,
                            srdelete_op: SRBucketDeleteOp::Purge,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(ApiError::from)?;
            }
            "purge-deleted-bucket" => {
                let _ = store
                    .delete_bucket(
                        &bucket,
                        &DeleteBucketOptions {
                            force: true,
                            srdelete_op: SRBucketDeleteOp::Purge,
                            ..Default::default()
                        },
                    )
                    .await;
            }
            _ => return Err(s3_error!(InvalidRequest, "unsupported site replication bucket operation")),
        }

        Ok(empty_response(StatusCode::OK))
    }
}

pub struct SRPeerReplicateIAMItemHandler {}

#[async_trait::async_trait]
impl Operation for SRPeerReplicateIAMItemHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        let item: SRIAMItem = read_site_replication_json(req, "", false).await?;
        apply_iam_item(item).await?;
        Ok(empty_response(StatusCode::OK))
    }
}

pub struct SRPeerReplicateBucketItemHandler {}

#[async_trait::async_trait]
impl Operation for SRPeerReplicateBucketItemHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        let item: SRBucketMeta = read_site_replication_json(req, "", false).await?;
        apply_bucket_meta_item(item).await?;
        Ok(empty_response(StatusCode::OK))
    }
}

pub struct SRPeerGetIDPSettingsHandler {}

#[async_trait::async_trait]
impl Operation for SRPeerGetIDPSettingsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationAddAction).await?;

        let mut settings = IDPSettings::default();
        if let Some(oidc) = get_oidc() {
            let providers = oidc.list_providers();
            settings.open_id.enabled = !providers.is_empty();
            settings.open_id.region = get_global_region().map(|region| region.to_string()).unwrap_or_default();

            for provider in providers {
                let Some(config) = oidc.get_provider_config(&provider.provider_id) else {
                    continue;
                };
                let provider_settings = OpenIDProviderSettings {
                    claim_name: config.claim_name.clone(),
                    claim_userinfo_enabled: false,
                    role_policy: config.role_policy.clone(),
                    client_id: config.client_id.clone(),
                    hashed_client_secret: hash_client_secret(config.client_secret.as_deref()),
                };

                let claim_provider_unset = settings.open_id.claim_provider.client_id.is_empty()
                    && settings.open_id.claim_provider.claim_name.is_empty()
                    && settings.open_id.claim_provider.role_policy.is_empty()
                    && settings.open_id.claim_provider.hashed_client_secret.is_empty();

                if provider.provider_id == "default" || claim_provider_unset {
                    settings.open_id.claim_provider = provider_settings.clone();
                } else {
                    settings.open_id.roles.insert(provider.provider_id.clone(), provider_settings);
                }
            }
        }
        let (ldap, ldap_configs) = load_ldap_idp_settings();
        settings.ldap = ldap;
        settings.ldap_configs = ldap_configs;

        json_response(&settings)
    }
}

pub struct SiteReplicationEditHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationEditHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationAddAction).await?;
        let ilm_expiry_override = sr_edit_ilm_expiry_override(&req.uri);
        let incoming: PeerInfo = read_site_replication_json(req, &cred.secret_key, true).await?;
        let current_state = load_site_replication_state().await?;
        let state = edit_state(current_state.clone(), incoming.clone(), ilm_expiry_override);

        if !current_state.service_account_access_key.is_empty() && !current_state.service_account_secret_key.is_empty() {
            let peers_to_send: Vec<PeerInfo> = if ilm_expiry_override.is_some() {
                state.peers.values().cloned().collect()
            } else {
                vec![normalize_peer_info(incoming)]
            };

            for target in current_state.peers.values() {
                let local_target = get_global_deployment_id()
                    .as_ref()
                    .is_some_and(|deployment_id| deployment_id == &target.deployment_id);
                if local_target {
                    continue;
                }

                for peer in &peers_to_send {
                    send_peer_admin_request(
                        &target.endpoint,
                        SITE_REPLICATION_PEER_EDIT_PATH,
                        &current_state.service_account_access_key,
                        &current_state.service_account_secret_key,
                        peer,
                    )
                    .await?;
                }
            }
        }

        save_site_replication_state(&state).await?;
        json_response(&ReplicateEditStatus {
            success: true,
            status: SITE_REPL_EDIT_SUCCESS.to_string(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
    }
}

pub struct SRPeerEditHandler {}

#[async_trait::async_trait]
impl Operation for SRPeerEditHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationAddAction).await?;
        let ilm_expiry_override = sr_edit_ilm_expiry_override(&req.uri);
        let state = load_site_replication_state().await?;
        let local_peer = current_local_peer(&req, &state);
        let mut incoming: PeerInfo = read_site_replication_json(req, "", false).await?;
        if same_endpoint(&incoming.endpoint, &local_peer.endpoint) {
            incoming.deployment_id = local_peer.deployment_id.clone();
            if incoming.name.is_empty() {
                incoming.name = local_peer.name.clone();
            }
        }
        let state =
            sync_state_name_for_local_peer(update_peer(state, incoming.clone(), ilm_expiry_override), &local_peer, &incoming);
        save_site_replication_state(&state).await?;
        Ok(empty_response(StatusCode::OK))
    }
}

pub struct SRPeerRemoveHandler {}

#[async_trait::async_trait]
impl Operation for SRPeerRemoveHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationRemoveAction).await?;
        let remove_req: SRRemoveReq = read_site_replication_json(req, "", false).await?;
        let state = remove_sites(load_site_replication_state().await?, remove_req);
        persist_site_replication_state(&state).await?;
        Ok(empty_response(StatusCode::OK))
    }
}

pub struct SiteReplicationResyncOpHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationResyncOpHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationResyncAction).await?;
        let operation = query_pairs(&req.uri).get("operation").cloned().unwrap_or_default();
        let peer: PeerInfo = read_site_replication_json(req, "", false).await?;
        let mut state = load_site_replication_state().await?;
        let local_peer = current_local_runtime_peer(&state);
        let peer = normalize_peer_info(peer);
        if peer.deployment_id == local_peer.deployment_id {
            return Err(s3_error!(InvalidRequest, "invalid peer specified - cannot resync to self"));
        }
        if !state.peers.contains_key(&peer.deployment_id) {
            return Err(s3_error!(InvalidRequest, "site replication peer not found"));
        }
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };
        let buckets = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;
        let bucket_names: Vec<String> = buckets.into_iter().map(|bucket| bucket.name).collect();

        let status = match operation.as_str() {
            SITE_REPL_RESYNC_START => {
                let mut status = resync_status_for_state(&mut state, &operation, &peer, vec![]);
                let mut bucket_statuses = Vec::new();
                for bucket in bucket_names {
                    bucket_statuses.push(start_site_bucket_resync(&bucket, &peer, &status.resync_id).await);
                }
                let failures = bucket_statuses.iter().filter(|bucket| bucket.status == "failed").count();
                if failures == bucket_statuses.len() && !bucket_statuses.is_empty() {
                    status.status = "failed".to_string();
                    status.err_detail = "all buckets resync failed".to_string();
                } else if failures > 0 {
                    status.err_detail = "partial failure in starting site resync".to_string();
                }
                status.buckets = bucket_statuses;
                state.resync_status.insert(peer.deployment_id.clone(), status.clone());
                status
            }
            SITE_REPL_RESYNC_CANCEL => {
                let Some(existing_status) = state.resync_status.get(&peer.deployment_id).cloned() else {
                    return Err(s3_error!(InvalidRequest, "no resync in progress"));
                };
                if existing_status.resync_id.is_empty() {
                    return Err(s3_error!(InvalidRequest, "no resync in progress"));
                }
                let mut status = SRResyncOpStatus {
                    op_type: operation.clone(),
                    resync_id: existing_status.resync_id.clone(),
                    status: "success".to_string(),
                    ..Default::default()
                };
                let mut bucket_statuses = Vec::new();
                for bucket in bucket_names {
                    bucket_statuses.push(cancel_site_bucket_resync(&bucket, &peer, &existing_status.resync_id).await);
                }
                let failures = bucket_statuses.iter().filter(|bucket| bucket.status == "failed").count();
                if failures == bucket_statuses.len() && !bucket_statuses.is_empty() {
                    status.status = "failed".to_string();
                    status.err_detail = "all buckets resync cancel failed".to_string();
                } else if failures > 0 {
                    status.err_detail = "partial failure in canceling site resync".to_string();
                }
                status.buckets = bucket_statuses;
                state.resync_status.insert(peer.deployment_id.clone(), status.clone());
                status
            }
            _ => return Err(s3_error!(InvalidRequest, "unsupported resync operation")),
        };
        save_site_replication_state(&state).await?;
        json_response(&status)
    }
}

pub struct SRStateEditHandler {}

#[async_trait::async_trait]
impl Operation for SRStateEditHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        let body: SRStateEditReq = read_site_replication_json(req, "", false).await?;
        let state = apply_state_edit_req(load_site_replication_state().await?, body);
        save_site_replication_state(&state).await?;
        Ok(empty_response(StatusCode::OK))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Uri;

    fn peer(name: &str, endpoint: &str) -> PeerInfo {
        PeerInfo {
            name: name.to_string(),
            endpoint: endpoint.to_string(),
            deployment_id: String::new(),
            sync_state: SyncStatus::Unknown,
            default_bandwidth: BucketBandwidth::default(),
            replicate_ilm_expiry: false,
            object_naming_mode: String::new(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        }
    }

    #[test]
    fn test_sr_status_options_parse_minio_query_flags() {
        let uri: Uri = "/rustfs/admin/v3/site-replication/status?buckets=true&policies=true&users=true&groups=true&metrics=true&peer-state=true&ilm-expiry-rules=true&entity=bucket&entityvalue=photos"
            .parse()
            .unwrap();

        let opts = sr_status_options(&uri);

        assert!(opts.buckets);
        assert!(opts.policies);
        assert!(opts.users);
        assert!(opts.groups);
        assert!(opts.metrics);
        assert!(opts.peer_state);
        assert!(opts.ilm_expiry_rules);
        assert_eq!(opts.entity, SREntityType::Bucket);
        assert_eq!(opts.entity_value, "photos");
    }

    #[test]
    fn test_query_flag_parses_lock_enabled() {
        let uri: Uri =
            "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning&lockEnabled=true"
                .parse()
                .unwrap();

        assert!(query_flag(&uri, "lockEnabled"));
        assert!(!query_flag(&uri, "missing"));
    }

    #[test]
    fn test_merge_add_sites_propagates_replicate_ilm_expiry() {
        let state = merge_add_sites(
            SiteReplicationState::default(),
            peer("local", "https://local.example.com"),
            vec![PeerSite {
                name: "remote".to_string(),
                endpoint: "https://remote.example.com".to_string(),
                access_key: "remote-ak".to_string(),
                secret_key: "remote-sk".to_string(),
            }],
            "svc-ak".to_string(),
            "svc-sk".to_string(),
            "root".to_string(),
            true,
        );

        assert!(state.peers.values().all(|peer| peer.replicate_ilm_expiry));
    }

    #[test]
    fn test_merge_add_sites_deduplicates_local_site_from_input() {
        let local_peer = PeerInfo {
            deployment_id: "local-dep".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let state = merge_add_sites(
            SiteReplicationState::default(),
            local_peer,
            vec![
                PeerSite {
                    name: "local".to_string(),
                    endpoint: "https://local.example.com/".to_string(),
                    access_key: "local-ak".to_string(),
                    secret_key: "local-sk".to_string(),
                },
                PeerSite {
                    name: "remote".to_string(),
                    endpoint: "https://remote.example.com".to_string(),
                    access_key: "remote-ak".to_string(),
                    secret_key: "remote-sk".to_string(),
                },
            ],
            "svc-ak".to_string(),
            "svc-sk".to_string(),
            "root".to_string(),
            true,
        );

        assert_eq!(state.peers.len(), 2);
        assert!(state.peers.contains_key("local-dep"));
    }

    #[test]
    fn test_normalize_join_peers_rewrites_local_endpoint_to_real_deployment_id() {
        let local_peer = PeerInfo {
            deployment_id: "real-local".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let peers = BTreeMap::from([
            (
                "hash-local".to_string(),
                PeerInfo {
                    deployment_id: "hash-local".to_string(),
                    ..peer("local", "https://local.example.com/")
                },
            ),
            (
                "hash-remote".to_string(),
                PeerInfo {
                    deployment_id: "hash-remote".to_string(),
                    ..peer("remote", "https://remote.example.com")
                },
            ),
        ]);

        let normalized = normalize_join_peers_for_local(&local_peer, peers);

        assert!(normalized.contains_key("real-local"));
        assert!(!normalized.contains_key("hash-local"));
        assert!(normalized.contains_key("hash-remote"));
    }

    #[test]
    fn test_reconcile_peer_with_actual_identity_replaces_endpoint_hash_key() {
        let mut state = SiteReplicationState::default();
        state.peers.insert(
            "local".to_string(),
            PeerInfo {
                deployment_id: "local".to_string(),
                ..peer("local", "https://local.example.com")
            },
        );
        state.peers.insert(
            "hash-remote".to_string(),
            PeerInfo {
                deployment_id: "hash-remote".to_string(),
                ..peer("remote", "https://remote.example.com")
            },
        );

        let reconciled = reconcile_peer_with_actual_identity(
            state,
            PeerInfo {
                deployment_id: "real-remote".to_string(),
                ..peer("remote", "https://remote.example.com/")
            },
        );

        assert!(reconciled.peers.contains_key("local"));
        assert!(reconciled.peers.contains_key("real-remote"));
        assert!(!reconciled.peers.contains_key("hash-remote"));
    }

    #[test]
    fn test_sync_state_name_for_local_peer_updates_top_level_name() {
        let mut state = SiteReplicationState {
            name: "old-local".to_string(),
            ..Default::default()
        };
        let local_peer = PeerInfo {
            deployment_id: "local".to_string(),
            ..peer("old-local", "https://local.example.com")
        };
        let incoming = PeerInfo {
            deployment_id: "local".to_string(),
            ..peer("new-local", "https://local.example.com/")
        };

        state = sync_state_name_for_local_peer(state, &local_peer, &incoming);

        assert_eq!(state.name, "new-local");
    }

    #[test]
    fn test_site_replication_state_requires_remote_peer_to_be_enabled() {
        let mut state = SiteReplicationState::default();
        state.peers.insert(
            "local".to_string(),
            PeerInfo {
                deployment_id: "local".to_string(),
                ..peer("local", "https://local.example.com")
            },
        );

        assert!(!state.enabled());
    }

    #[test]
    fn test_sr_remove_req_accepts_null_sites() {
        let req: SRRemoveReq = serde_json::from_str(r#"{"all":true,"sites":null}"#).expect("parse remove req");

        assert!(req.remove_all);
        assert!(req.site_names.is_empty());
    }

    #[test]
    fn test_update_peer_respects_ilm_expiry_override() {
        let peer = peer("remote", "https://remote.example.com");

        let state = update_peer(SiteReplicationState::default(), peer, Some(true));

        assert!(state.peers.values().next().unwrap().replicate_ilm_expiry);
    }

    #[test]
    fn test_edit_state_updates_ilm_expiry_for_all_peers() {
        let mut state = SiteReplicationState::default();
        state.peers.insert(
            "local".to_string(),
            PeerInfo {
                deployment_id: "local".to_string(),
                ..peer("local", "https://local.example.com")
            },
        );
        state.peers.insert(
            "remote".to_string(),
            PeerInfo {
                deployment_id: "remote".to_string(),
                ..peer("remote", "https://remote.example.com")
            },
        );

        let edited = edit_state(state, PeerInfo::default(), Some(true));

        assert!(edited.peers.values().all(|peer| peer.replicate_ilm_expiry));
    }

    #[test]
    fn test_bucket_target_matches_peer_by_deployment_id() {
        let target = BucketTarget {
            deployment_id: "remote-dep".to_string(),
            endpoint: "other-host:9000".to_string(),
            target_type: BucketTargetType::ReplicationService,
            ..Default::default()
        };
        let mut remote = peer("remote", "https://remote.example.com");
        remote.deployment_id = "remote-dep".to_string();

        assert!(bucket_target_matches_peer(&target, &remote));
    }

    #[test]
    fn test_bucket_target_matches_peer_by_endpoint() {
        let target = BucketTarget {
            endpoint: "remote.example.com:443".to_string(),
            secure: true,
            target_type: BucketTargetType::ReplicationService,
            ..Default::default()
        };
        let remote = peer("remote", "https://remote.example.com/");

        assert!(bucket_target_matches_peer(&target, &remote));
    }

    #[test]
    fn test_peer_deployment_id_for_endpoint_matches_normalized_endpoint() {
        let mut state = SiteReplicationState::default();
        let mut remote = peer("remote", "https://remote.example.com");
        remote.deployment_id = "remote-dep".to_string();
        state.peers.insert(remote.deployment_id.clone(), remote);

        let deployment_id = peer_deployment_id_for_endpoint(&state, "https://remote.example.com/");

        assert_eq!(deployment_id.as_deref(), Some("remote-dep"));
    }

    #[test]
    fn test_reconcile_site_replication_bucket_targets_upserts_remote_peer_targets() {
        let mut state = SiteReplicationState::default();
        state.service_account_access_key = "site-replicator-0".to_string();
        state.service_account_secret_key = "secret".to_string();
        state.peers.insert(
            "local".to_string(),
            PeerInfo {
                deployment_id: "local".to_string(),
                ..peer("local", "https://local.example.com")
            },
        );
        state.peers.insert(
            "remote".to_string(),
            PeerInfo {
                deployment_id: "remote".to_string(),
                ..peer("remote", "http://remote.example.com:9000")
            },
        );

        let targets = reconcile_site_replication_bucket_targets(
            BucketTargets::default(),
            "photos",
            &state,
            &PeerInfo {
                deployment_id: "local".to_string(),
                ..peer("local", "https://local.example.com")
            },
            None,
        );

        assert_eq!(targets.targets.len(), 1);
        let target = &targets.targets[0];
        assert_eq!(target.target_type, BucketTargetType::ReplicationService);
        assert_eq!(target.endpoint, "remote.example.com:9000");
        assert!(!target.secure);
        assert_eq!(target.target_bucket, "photos");
        assert_eq!(target.deployment_id, "remote");
        assert_eq!(target.arn, "arn:rustfs:replication::remote:photos");
        let credentials = target
            .credentials
            .as_ref()
            .expect("site replication target should carry credentials");
        assert_eq!(credentials.access_key, "site-replicator-0");
        assert_eq!(credentials.secret_key, "secret");
    }

    #[test]
    fn test_apply_state_edit_req_only_updates_ilm_expiry_flags() {
        let mut state = SiteReplicationState::default();
        let mut remote = peer("remote", "https://remote.example.com");
        remote.deployment_id = "remote".to_string();
        remote.object_naming_mode = "uuid".to_string();
        state.peers.insert(remote.deployment_id.clone(), remote);
        state.updated_at = Some(OffsetDateTime::UNIX_EPOCH);

        let edited = apply_state_edit_req(
            state,
            SRStateEditReq {
                peers: BTreeMap::from([(
                    "remote".to_string(),
                    PeerInfo {
                        deployment_id: "remote".to_string(),
                        replicate_ilm_expiry: true,
                        object_naming_mode: "should-not-overwrite".to_string(),
                        ..peer("remote", "https://remote.example.com")
                    },
                )]),
                updated_at: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(10)),
            },
        );

        assert!(edited.peers["remote"].replicate_ilm_expiry);
        assert_eq!(edited.peers["remote"].object_naming_mode, "uuid");
    }

    #[test]
    fn test_apply_state_edit_req_ignores_stale_updates() {
        let mut state = SiteReplicationState::default();
        let mut remote = peer("remote", "https://remote.example.com");
        remote.deployment_id = "remote".to_string();
        state.peers.insert(remote.deployment_id.clone(), remote);
        state.updated_at = Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20));

        let edited = apply_state_edit_req(
            state.clone(),
            SRStateEditReq {
                peers: BTreeMap::from([(
                    "remote".to_string(),
                    PeerInfo {
                        deployment_id: "remote".to_string(),
                        replicate_ilm_expiry: true,
                        ..peer("remote", "https://remote.example.com")
                    },
                )]),
                updated_at: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(10)),
            },
        );

        assert_eq!(edited.updated_at, state.updated_at);
        assert!(!edited.peers["remote"].replicate_ilm_expiry);
    }

    #[test]
    fn test_filter_sr_info_keeps_only_requested_entity() {
        let mut info = SRInfo::default();
        info.buckets.insert("photos".to_string(), SRBucketInfo::default());
        info.buckets.insert("logs".to_string(), SRBucketInfo::default());
        info.policies.insert("readonly".to_string(), SRIAMPolicy::default());

        let filtered = filter_sr_info(
            info,
            &SRStatusOptions {
                entity: SREntityType::Bucket,
                entity_value: "photos".to_string(),
                ..Default::default()
            },
        );

        assert!(filtered.buckets.contains_key("photos"));
        assert!(!filtered.buckets.contains_key("logs"));
        assert!(filtered.policies.is_empty());
    }

    #[test]
    fn test_hash_client_secret_matches_minio_style_base64url_sha256() {
        assert_eq!(hash_client_secret(Some("secret")), "K7gNU3sdo-OL0wNhqoVWhr3g6s1xYv72ol_pe_Unols");
    }

    #[test]
    fn test_ldap_settings_from_kvs_reads_minio_style_keys() {
        let kvs = rustfs_ecstore::config::KVS(vec![
            rustfs_ecstore::config::KV {
                key: "enable".to_string(),
                value: "on".to_string(),
                hidden_if_empty: false,
            },
            rustfs_ecstore::config::KV {
                key: "user_dn_search_base_dn".to_string(),
                value: "ou=people,dc=example,dc=com".to_string(),
                hidden_if_empty: false,
            },
            rustfs_ecstore::config::KV {
                key: "user_dn_search_filter".to_string(),
                value: "(uid=%s)".to_string(),
                hidden_if_empty: false,
            },
            rustfs_ecstore::config::KV {
                key: "group_search_base_dn".to_string(),
                value: "ou=groups,dc=example,dc=com".to_string(),
                hidden_if_empty: false,
            },
            rustfs_ecstore::config::KV {
                key: "group_search_filter".to_string(),
                value: "(&(objectclass=groupOfNames)(member=%s))".to_string(),
                hidden_if_empty: false,
            },
        ]);

        let (ldap, ldap_configs) = ldap_settings_from_kvs(&kvs);

        assert!(ldap.is_ldap_enabled);
        assert_eq!(ldap.ldap_user_dn_search_base, "ou=people,dc=example,dc=com");
        assert_eq!(ldap.ldap_user_dn_search_filter, "(uid=%s)");
        assert_eq!(ldap.ldap_group_search_base, "ou=groups,dc=example,dc=com");
        assert_eq!(ldap.ldap_group_search_filter, "(&(objectclass=groupOfNames)(member=%s))");
        assert!(ldap_configs.enabled);
        assert!(ldap_configs.configs.contains_key("default"));
    }

    #[test]
    fn test_gob_site_netperf_node_result_matches_go_encoding() {
        let data = encode_go_gob_site_netperf_node_result(&SiteNetPerfNodeResult {
            endpoint: "https://peer.example.com".to_string(),
            tx: 123,
            tx_total_duration_ns: 456,
            rx: 789,
            rx_total_duration_ns: 321,
            total_conn: 3,
            error: String::new(),
        });

        let expected: &[u8] = &[
            0x7d, 0x7f, 0x03, 0x01, 0x01, 0x15, 0x53, 0x69, 0x74, 0x65, 0x4e, 0x65, 0x74, 0x50, 0x65, 0x72, 0x66, 0x4e, 0x6f,
            0x64, 0x65, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x01, 0xff, 0x80, 0x00, 0x01, 0x07, 0x01, 0x08, 0x45, 0x6e, 0x64,
            0x70, 0x6f, 0x69, 0x6e, 0x74, 0x01, 0x0c, 0x00, 0x01, 0x02, 0x54, 0x58, 0x01, 0x06, 0x00, 0x01, 0x0f, 0x54, 0x58,
            0x54, 0x6f, 0x74, 0x61, 0x6c, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x01, 0x04, 0x00, 0x01, 0x02, 0x52,
            0x58, 0x01, 0x06, 0x00, 0x01, 0x0f, 0x52, 0x58, 0x54, 0x6f, 0x74, 0x61, 0x6c, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69,
            0x6f, 0x6e, 0x01, 0x04, 0x00, 0x01, 0x09, 0x54, 0x6f, 0x74, 0x61, 0x6c, 0x43, 0x6f, 0x6e, 0x6e, 0x01, 0x06, 0x00,
            0x01, 0x05, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x01, 0x0c, 0x00, 0x00, 0x00, 0x2d, 0xff, 0x80, 0x01, 0x18, 0x68, 0x74,
            0x74, 0x70, 0x73, 0x3a, 0x2f, 0x2f, 0x70, 0x65, 0x65, 0x72, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e,
            0x63, 0x6f, 0x6d, 0x01, 0x7b, 0x01, 0xfe, 0x03, 0x90, 0x01, 0xfe, 0x03, 0x15, 0x01, 0xfe, 0x02, 0x82, 0x01, 0x03,
            0x00,
        ];

        assert_eq!(data, expected);
    }

    #[test]
    fn test_group_info_with_empty_members_still_requires_group_upsert() {
        let update = rustfs_madmin::GroupAddRemove {
            group: "empty-group".to_string(),
            members: vec![],
            status: GroupStatus::Enabled,
            is_remove: false,
        };

        assert!(group_info_requires_upsert(&update));
    }
}
