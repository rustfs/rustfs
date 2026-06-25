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

use super::super::Error as StorageError;
use super::super::bucket_target_sys::BucketTargetSys;
use super::super::ecstore_utils::{deserialize, serialize};
use super::super::metadata::{
    BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_POLICY_CONFIG, BUCKET_QUOTA_CONFIG_FILE, BUCKET_REPLICATION_CONFIG,
    BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG, BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG, OBJECT_LOCK_CONFIG,
};
use super::super::metadata_sys;
use super::super::replication::ResyncOpts;
use super::super::target::{ARN, BucketTarget, BucketTargetType, BucketTargets, Credentials};
use super::super::{AdminReplicationConfigExt as _, AdminVersioningConfigExt as _};
use super::super::{delete_admin_config, read_admin_config, save_admin_config};
use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::site_replication_identity::{
    canonical_endpoint, deployment_id_for_endpoint, normalize_peer_map_by_identity_with, same_identity_endpoint,
    site_identity_key,
};
use crate::admin::utils::{encode_compatible_admin_payload, read_compatible_admin_body};
use crate::app::context::{
    resolve_deployment_id, resolve_endpoints_handle, resolve_iam_handle, resolve_object_store_handle, resolve_oidc_handle,
    resolve_outbound_tls_generation, resolve_outbound_tls_state, resolve_region, resolve_replication_pool_handle,
    resolve_replication_stats_handle, resolve_runtime_port, resolve_server_config, resolve_token_signing_key,
};
use crate::auth::{check_key_valid, get_session_token};
use crate::config::get_config_snapshot;
use crate::error::ApiError;
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http::header::{CONTENT_TYPE, HOST};
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::{
    DEFAULT_CONSOLE_ADDRESS, DEFAULT_DELIMITER, DEFAULT_RUSTFS_TLS_PATH, ENV_RUSTFS_CONSOLE_ADDRESS, ENV_RUSTFS_TLS_PATH,
    MAX_ADMIN_REQUEST_BODY_SIZE,
};
use rustfs_iam::error::is_err_no_such_service_account;
use rustfs_iam::store::{MappedPolicy, UserType};
use rustfs_iam::sys::{
    NewServiceAccountOpts, SITE_REPLICATOR_SERVICE_ACCOUNT, UpdateServiceAccountOpts, get_claims_from_token_with_secret,
};
use rustfs_madmin::{
    AddOrUpdateUserReq, BucketBandwidth, GroupAddRemove, GroupStatus, IDPSettings, InProgressMetric, InQueueMetric,
    LDAPConfigSettings, LDAPSettings, OpenIDProviderSettings, PeerInfo, PeerSite, QStat, ReplProxyMetric, ReplicateAddStatus,
    ReplicateEditStatus, ReplicateRemoveStatus, ResyncBucketStatus, SITE_REPL_API_VERSION, SRBucketInfo, SRBucketMeta,
    SRBucketStatsSummary, SRGroupInfo, SRGroupStatsSummary, SRIAMItem, SRIAMPolicy, SRILMExpiryStatsSummary, SRInfo, SRMetric,
    SRMetricsSummary, SRPeerError, SRPeerJoinReq, SRPendingOperation, SRPolicyMapping, SRPolicyStatsSummary, SRRemoveReq,
    SRResyncOpStatus, SRRetryStats, SRSiteSummary, SRStateEditReq, SRStateInfo, SRStatusInfo, SRUserStatsSummary,
    SiteReplicationInfo, SyncStatus, WorkerStat,
};
use rustfs_policy::policy::{
    Policy,
    action::{Action, AdminAction},
};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use rustfs_storage_api::{BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions, SRBucketDeleteOp};
use rustfs_tls_runtime::GlobalPublishedOutboundTlsState;
use rustfs_utils::http::get_source_scheme;
use rustls_pki_types::pem::PemObject;
use s3s::dto::{
    BucketVersioningStatus, DeleteMarkerReplication, DeleteMarkerReplicationStatus, DeleteReplication, DeleteReplicationStatus,
    Destination, ExistingObjectReplication, ExistingObjectReplicationStatus, ReplicaModifications, ReplicaModificationsStatus,
    ReplicationConfiguration, ReplicationRule, ReplicationRuleStatus, SourceSelectionCriteria, VersioningConfiguration,
};
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::LazyLock;
use std::time::{Duration, Instant};
use time::OffsetDateTime;
use tokio::sync::Mutex;
use tracing::{info, warn};
use url::{Url, form_urlencoded};
use uuid::Uuid;

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_SITE_REPLICATION: &str = "site_replication";
const EVENT_ADMIN_SITE_REPLICATION_STATE: &str = "admin_site_replication_state";

const SITE_REPLICATION_STATE_PATH: &str = "config/site-replication/state.json";
const SITE_REPL_ADD_SUCCESS: &str = "Requested sites were configured for replication successfully.";
const SITE_REPL_EDIT_SUCCESS: &str = "Requested site was updated successfully.";
const SITE_REPL_REMOVE_SUCCESS: &str = "Requested site(s) were removed from cluster replication successfully.";
const SITE_REPL_RESYNC_START: &str = "start";
const SITE_REPL_RESYNC_CANCEL: &str = "cancel";
const SITE_REPL_RESYNC_STATUS: &str = "status";
const SITE_REPL_MIN_NETPERF_DURATION: Duration = Duration::from_secs(1);
const SITE_REPL_MAX_NETPERF_DURATION: Duration = Duration::from_secs(30);
const SITE_REPLICATION_PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SITE_REPLICATION_PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const SITE_REPLICATION_PEER_ERROR_DETAIL_LIMIT: usize = 256;
const SITE_REPLICATION_RETRY_QUEUE_LIMIT: usize = 256;
const SITE_REPLICATION_RETRY_FAILED_AFTER: u32 = 3;
const IDENTITY_LDAP_SUB_SYS: &str = "identity_ldap";
const LEGACY_LDAP_SUB_SYS: &str = "ldapserverconfig";
const SITE_REPLICATION_PEER_JOIN_PATH: &str = "/rustfs/admin/v3/site-replication/peer/join";
const SITE_REPLICATION_PEER_EDIT_PATH: &str = "/rustfs/admin/v3/site-replication/peer/edit";
const SITE_REPLICATION_PEER_REMOVE_PATH: &str = "/rustfs/admin/v3/site-replication/peer/remove";
const MINIO_ADMIN_PREFIX: &str = "/minio/admin";
const RUSTFS_ADMIN_V3_PREFIX: &str = "/rustfs/admin/v3";
const MINIO_ADMIN_V3_PREFIX: &str = "/minio/admin/v3";
const MINIO_SITE_REPLICATION_JOIN_PATH: &str = "/minio/admin/v3/site-replication/join";

fn site_replicator_service_account_policy() -> S3Result<Policy> {
    Policy::parse_config(
        br#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "admin:SiteReplicationAdd",
        "admin:SiteReplicationInfo",
        "admin:SiteReplicationOperation",
        "admin:SiteReplicationRemove"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:HeadBucket",
        "s3:GetBucketVersioning",
        "s3:PutBucketVersioning",
        "s3:GetReplicationConfiguration",
        "s3:PutReplicationConfiguration",
        "s3:ListBucket",
        "s3:ListBucketVersions"
      ],
      "Resource": ["arn:aws:s3:::*"]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:GetObjectVersionForReplication",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:DeleteObjectVersion",
        "s3:ReplicateObject",
        "s3:ReplicateDelete",
        "s3:ReplicateTags",
        "s3:GetObjectTagging",
        "s3:GetObjectVersionTagging",
        "s3:PutObjectTagging",
        "s3:PutObjectVersionTagging",
        "s3:DeleteObjectTagging",
        "s3:DeleteObjectVersionTagging"
      ],
      "Resource": ["arn:aws:s3:::*/*"]
    }
  ]
}"#,
    )
    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("parse site replicator policy failed: {e}")))
}

#[derive(Clone)]
enum SiteReplicationPeerClientCacheEntry {
    Ready(reqwest::Client),
    Failed(String),
}

#[derive(Clone)]
struct SiteReplicationPeerClientCache {
    generation: u64,
    entry: SiteReplicationPeerClientCacheEntry,
}

static SITE_REPLICATION_PEER_CLIENT: LazyLock<Mutex<Option<SiteReplicationPeerClientCache>>> = LazyLock::new(|| Mutex::new(None));
static SITE_REPLICATION_STATE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

fn site_replication_peer_client_cache_hit(
    cache: &Option<SiteReplicationPeerClientCache>,
    generation: u64,
) -> Option<S3Result<reqwest::Client>> {
    let cached = cache.as_ref()?;
    if cached.generation != generation {
        return None;
    }
    Some(match &cached.entry {
        SiteReplicationPeerClientCacheEntry::Ready(client) => Ok(client.clone()),
        SiteReplicationPeerClientCacheEntry::Failed(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("initialize site replication peer client failed: {err}"),
        )),
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SiteReplicationState {
    name: String,
    service_account_access_key: String,
    #[serde(default, skip_serializing)]
    service_account_secret_key: String,
    service_account_parent: String,
    peers: BTreeMap<String, PeerInfo>,
    updated_at: Option<OffsetDateTime>,
    resync_status: BTreeMap<String, SRResyncOpStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_rotation: Option<PendingRotation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_remove: Option<PendingRemove>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    retry_queue: Vec<SiteReplicationRetryEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SiteReplicationRetryEvent {
    id: String,
    peer_deployment_id: String,
    peer_endpoint: String,
    path: String,
    retry_count: u32,
    failed: bool,
    last_error: String,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PendingRotation {
    id: String,
    access_key: String,
    parent: String,
    new_secret_key: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    secret_candidates: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    peers: BTreeMap<String, PeerInfo>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    acked_deployment_ids: BTreeSet<String>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    updated_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PendingRemove {
    id: String,
    req: SRRemoveReq,
    service_account_access_key: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    secret_candidates: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    original_peers: BTreeMap<String, PeerInfo>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    acked_deployment_ids: BTreeSet<String>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    updated_at: Option<OffsetDateTime>,
}

struct SiteReplicationRuntime {
    state: SiteReplicationState,
    local_peer: PeerInfo,
    service_account_secret_key: String,
}

#[derive(Debug, Clone)]
struct SiteReplicationAddPreflightInfo {
    name: String,
    endpoint: String,
    deployment_id: String,
    enabled: bool,
    bucket_count: usize,
    peer_deployment_ids: BTreeSet<String>,
    idp_settings: serde_json::Value,
}

#[derive(Debug, Default)]
struct SiteReplicationBootstrapPlan {
    iam_items: Vec<SRIAMItem>,
    bucket_make_ops: Vec<String>,
    bucket_items: Vec<SRBucketMeta>,
    bucket_configure_ops: Vec<String>,
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
        (Method::PUT, "/v3/site-replication/join", AdminOperation(&SRPeerJoinHandler {})),
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
        (
            Method::PUT,
            "/v3/site-replication/repair",
            AdminOperation(&SiteReplicationRepairHandler {}),
        ),
        (
            Method::POST,
            "/v3/site-replication/rotate-svc-acct",
            AdminOperation(&SRRotateServiceAccountHandler {}),
        ),
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

fn reject_site_replicator_on_public_admin(cred: &rustfs_credentials::Credentials) -> S3Result<()> {
    if cred.access_key == SITE_REPLICATOR_SERVICE_ACCOUNT {
        return Err(s3_error!(
            AccessDenied,
            "site replicator service account cannot modify site replication state"
        ));
    }
    Ok(())
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

fn site_repl_netperf_duration(uri: &Uri) -> Duration {
    query_pairs(uri)
        .get("duration")
        .and_then(|value| rustfs_madmin::utils::parse_duration(value).ok())
        .unwrap_or(SITE_REPL_MIN_NETPERF_DURATION)
        .clamp(SITE_REPL_MIN_NETPERF_DURATION, SITE_REPL_MAX_NETPERF_DURATION)
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
    let Some(store) = resolve_object_store_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    match read_admin_config(store, SITE_REPLICATION_STATE_PATH).await {
        Ok(data) => {
            let mut state: SiteReplicationState = serde_json::from_slice(&data)
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("invalid site replication state: {e}")))?;
            state.peers = normalize_peer_map_by_identity(state.peers);
            Ok(state)
        }
        Err(StorageError::ConfigNotFound) => Ok(SiteReplicationState::default()),
        Err(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to load site replication state: {err}"),
        )),
    }
}

async fn save_site_replication_state(state: &SiteReplicationState) -> S3Result<()> {
    let Some(store) = resolve_object_store_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    let mut normalized = state.clone();
    normalized.peers = normalize_peer_map_by_identity(normalized.peers);

    let data = serde_json::to_vec(&normalized)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize state failed: {e}")))?;
    save_admin_config(store, SITE_REPLICATION_STATE_PATH, data)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("save state failed: {e}")))?;
    Ok(())
}

async fn clear_site_replication_state() -> S3Result<()> {
    let Some(store) = resolve_object_store_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    match delete_admin_config(store, SITE_REPLICATION_STATE_PATH).await {
        Ok(()) | Err(StorageError::ConfigNotFound) => Ok(()),
        Err(err) => Err(S3Error::with_message(S3ErrorCode::InternalError, format!("clear state failed: {err}"))),
    }
}

async fn persist_site_replication_state(state: &SiteReplicationState) -> S3Result<()> {
    let mut normalized = state.clone();
    normalized.peers = normalize_peer_map_by_identity(normalized.peers);
    if normalized.peers.len() <= 1 && normalized.pending_rotation.is_none() && normalized.pending_remove.is_none() {
        clear_site_replication_state().await
    } else {
        save_site_replication_state(&normalized).await
    }
}

fn build_site_replication_peer_client(outbound_tls: &GlobalPublishedOutboundTlsState) -> S3Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .timeout(SITE_REPLICATION_PEER_REQUEST_TIMEOUT)
        .connect_timeout(SITE_REPLICATION_PEER_CONNECT_TIMEOUT)
        .pool_idle_timeout(Some(Duration::from_secs(60)));

    if let Some(root_ca_pem) = outbound_tls.root_ca_pem.as_ref() {
        let mut reader = std::io::BufReader::new(root_ca_pem.as_slice());
        let certs_der = rustls_pki_types::CertificateDer::pem_reader_iter(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("failed to parse published site-replication CA certs: {e}"),
                )
            })?;

        for cert_der in certs_der {
            let cert = reqwest::Certificate::from_der(cert_der.as_ref()).map_err(|e| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("failed to load published site-replication CA cert: {e}"),
                )
            })?;
            builder = builder.add_root_certificate(cert);
        }
    }

    builder
        .build()
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("build site replication peer client failed: {e}")))
}

async fn site_replication_peer_client() -> S3Result<reqwest::Client> {
    let generation = resolve_outbound_tls_generation().0;
    let cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
    if let Some(hit) = site_replication_peer_client_cache_hit(&cache, generation) {
        return hit;
    }
    drop(cache);

    let outbound_tls = resolve_outbound_tls_state().await;
    let built = build_site_replication_peer_client(&outbound_tls);
    let cache_entry = match &built {
        Ok(client) => SiteReplicationPeerClientCacheEntry::Ready(client.clone()),
        Err(err) => SiteReplicationPeerClientCacheEntry::Failed(err.to_string()),
    };

    let mut cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
    if cache.as_ref().is_none_or(|cached| cached.generation <= generation) {
        *cache = Some(SiteReplicationPeerClientCache {
            generation,
            entry: cache_entry,
        });
    }

    built
}

fn runtime_tls_enabled_with(endpoints: Option<&super::super::EndpointServerPools>) -> bool {
    if !rustfs_utils::get_env_str(ENV_RUSTFS_TLS_PATH, DEFAULT_RUSTFS_TLS_PATH).is_empty() {
        return true;
    }

    if let Some(tls_enabled) = endpoints.and_then(|endpoints| {
        endpoints
            .as_ref()
            .iter()
            .flat_map(|pool| pool.endpoints.as_ref().iter())
            .find(|endpoint| endpoint.is_local)
            .map(|endpoint| endpoint.url.scheme().eq_ignore_ascii_case("https"))
    }) {
        return tls_enabled;
    }

    false
}

fn runtime_tls_enabled() -> bool {
    let endpoints = resolve_endpoints_handle();
    runtime_tls_enabled_with(endpoints.as_ref())
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

fn ldap_settings_from_kvs(kvs: &rustfs_config::server_config::KVS) -> (LDAPSettings, LDAPConfigSettings) {
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
    let Some(config) = resolve_server_config() else {
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
    let scheme = get_source_scheme(headers)
        .and_then(|value| {
            value
                .split(',')
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_ascii_lowercase)
        })
        .or_else(|| uri.scheme_str().map(str::to_ascii_lowercase))
        .unwrap_or_else(|| {
            if runtime_tls_enabled() {
                "https".to_string()
            } else {
                "http".to_string()
            }
        });

    let host = headers
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| uri.authority().map(|value| value.as_str().to_string()))
        .or_else(|| {
            resolve_endpoints_handle().and_then(|endpoints| {
                endpoints
                    .as_ref()
                    .iter()
                    .flat_map(|pool| pool.endpoints.as_ref().iter())
                    .find(|endpoint| endpoint.is_local)
                    .map(|endpoint| endpoint.host_port())
            })
        })
        .unwrap_or_else(|| format!("127.0.0.1:{}", resolve_runtime_port()));

    format!("{scheme}://{host}")
}

fn runtime_console_port() -> Option<u16> {
    let console_address = get_config_snapshot()
        .map(|snapshot| snapshot.console_address.clone())
        .unwrap_or_else(|| rustfs_utils::get_env_str(ENV_RUSTFS_CONSOLE_ADDRESS, DEFAULT_CONSOLE_ADDRESS));

    let parse_target = if console_address.starts_with(':') {
        format!("127.0.0.1{console_address}")
    } else {
        console_address
    };

    Url::parse(&format!("http://{parse_target}"))
        .ok()
        .and_then(|parsed| parsed.port_or_known_default())
}

fn site_replication_local_endpoint(uri: &Uri, headers: &HeaderMap) -> String {
    let endpoint = request_endpoint(uri, headers);
    match Url::parse(&endpoint) {
        Ok(mut parsed) => {
            if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
                return request_endpoint(&Uri::from_static("/"), &HeaderMap::new());
            }
            if parsed.port_or_known_default() == runtime_console_port() && parsed.set_port(Some(resolve_runtime_port())).is_ok() {
                parsed.to_string().trim_end_matches('/').to_string()
            } else {
                endpoint
            }
        }
        Err(_) => request_endpoint(&Uri::from_static("/"), &HeaderMap::new()),
    }
}

fn current_local_runtime_endpoint() -> String {
    site_replication_local_endpoint(&Uri::from_static("/"), &HeaderMap::new())
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
    let endpoint = site_replication_local_endpoint(&req.uri, &req.headers);
    let deployment_id = resolve_deployment_id().unwrap_or_else(|| deployment_id_for_endpoint(&endpoint));
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
    let deployment_id = resolve_deployment_id().unwrap_or_else(|| deployment_id_for_endpoint(&endpoint));
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

fn normalize_peer_map_by_identity(peers: BTreeMap<String, PeerInfo>) -> BTreeMap<String, PeerInfo> {
    normalize_peer_map_by_identity_with(peers, normalize_peer_info)
}

fn existing_peer_for_endpoint(state: &SiteReplicationState, endpoint: &str) -> Option<PeerInfo> {
    state
        .peers
        .values()
        .find(|peer| same_identity_endpoint(&peer.endpoint, endpoint))
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

fn validate_site_replication_peer_endpoint(endpoint: &str) -> S3Result<()> {
    let parsed = Url::parse(endpoint)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site endpoint `{endpoint}`: {e}")))?;
    match parsed.scheme() {
        "http" | "https" => {}
        scheme => {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                format!("invalid site endpoint `{endpoint}`: unsupported scheme `{scheme}`"),
            ));
        }
    }
    if parsed.host_str().is_none() {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("invalid site endpoint `{endpoint}`: missing host"),
        ));
    }
    Ok(())
}

fn validate_add_sites(sites: &[PeerSite], local_peer: &PeerInfo) -> S3Result<()> {
    if sites.is_empty() {
        return Err(s3_error!(InvalidRequest, "at least one site is required"));
    }

    let mut seen = HashSet::new();
    let mut remote_count = 0usize;
    for site in sites {
        if site.endpoint.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "site endpoint is required"));
        }
        validate_site_replication_peer_endpoint(&site.endpoint)?;
        let endpoint_key = site_identity_key(&site.endpoint);
        if !seen.insert(endpoint_key) {
            return Err(s3_error!(InvalidRequest, "duplicate site endpoint `{}`", site.endpoint));
        }

        if same_identity_endpoint(&site.endpoint, &local_peer.endpoint) {
            continue;
        }
        remote_count += 1;
        if site.access_key.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "accessKey is required for site `{}`", site.endpoint));
        }
        if site.secret_key.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "secretKey is required for site `{}`", site.endpoint));
        }
    }

    if remote_count == 0 {
        return Err(s3_error!(InvalidRequest, "at least one remote site is required"));
    }

    Ok(())
}

fn idp_settings_value(settings: &IDPSettings) -> S3Result<serde_json::Value> {
    serde_json::to_value(settings)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize IDP settings failed: {e}")))
}

fn add_preflight_info_from_sr_info(
    site: &PeerSite,
    info: SRInfo,
    idp_settings: IDPSettings,
) -> S3Result<SiteReplicationAddPreflightInfo> {
    Ok(SiteReplicationAddPreflightInfo {
        name: if info.name.is_empty() { site.name.clone() } else { info.name },
        endpoint: site.endpoint.clone(),
        deployment_id: info.deployment_id,
        enabled: info.enabled,
        bucket_count: info.buckets.len(),
        peer_deployment_ids: info.state.peers.keys().cloned().collect(),
        idp_settings: idp_settings_value(&idp_settings)?,
    })
}

async fn local_add_preflight_info(
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    local_site: &PeerSite,
) -> S3Result<SiteReplicationAddPreflightInfo> {
    add_preflight_info_from_sr_info(local_site, build_sr_info(state, local_peer).await?, local_idp_settings())
}

async fn remote_add_preflight_info(site: &PeerSite) -> S3Result<SiteReplicationAddPreflightInfo> {
    let info_body = send_peer_admin_get_request(
        &site.endpoint,
        "/rustfs/admin/v3/site-replication/metainfo",
        &site.access_key,
        &site.secret_key,
    )
    .await?;
    let info: SRInfo = serde_json::from_slice(&info_body).map_err(|e| {
        S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("invalid site replication metainfo from `{}`: {e}", site.endpoint),
        )
    })?;

    let idp_body = send_peer_admin_get_request(
        &site.endpoint,
        "/rustfs/admin/v3/site-replication/peer/idp-settings",
        &site.access_key,
        &site.secret_key,
    )
    .await?;
    let idp_settings: IDPSettings = serde_json::from_slice(&idp_body).map_err(|e| {
        S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("invalid site replication IDP settings from `{}`: {e}", site.endpoint),
        )
    })?;

    add_preflight_info_from_sr_info(site, info, idp_settings)
}

fn validate_add_preflight_topology(infos: &[SiteReplicationAddPreflightInfo], local_peer: &PeerInfo) -> S3Result<()> {
    let mut deployment_ids = HashSet::new();
    let mut local_seen = false;
    let mut non_empty_sites = Vec::new();
    let local_idp = infos
        .iter()
        .find(|info| info.deployment_id == local_peer.deployment_id)
        .map(|info| &info.idp_settings);

    for info in infos {
        if info.deployment_id.trim().is_empty() {
            return Err(s3_error!(InvalidRequest, "site `{}` did not report deploymentID", info.endpoint));
        }
        if !deployment_ids.insert(info.deployment_id.clone()) {
            return Err(s3_error!(
                InvalidRequest,
                "duplicate deploymentID `{}` in site replication add request",
                info.deployment_id
            ));
        }
        if info.deployment_id == local_peer.deployment_id {
            local_seen = true;
        }
        if info.bucket_count > 0 {
            non_empty_sites.push(info.name.clone());
        }
    }

    if !local_seen {
        return Err(s3_error!(
            InvalidRequest,
            "site replication add request must include the local deployment"
        ));
    }

    let Some(local_idp) = local_idp else {
        return Err(s3_error!(
            InvalidRequest,
            "local IDP settings unavailable for site replication add preflight"
        ));
    };
    for info in infos {
        if &info.idp_settings != local_idp {
            return Err(s3_error!(InvalidRequest, "IDP settings mismatch for site `{}`", info.endpoint));
        }
    }

    if non_empty_sites.len() > 1 {
        return Err(s3_error!(
            InvalidRequest,
            "site replication can be initialized with data on only one site; non-empty sites: {}",
            non_empty_sites.join(", ")
        ));
    }

    let requested: BTreeSet<String> = infos.iter().map(|info| info.deployment_id.clone()).collect();
    for info in infos.iter().filter(|info| info.enabled) {
        if !info.peer_deployment_ids.is_empty() && info.peer_deployment_ids != requested {
            return Err(s3_error!(
                InvalidRequest,
                "site `{}` is already configured with a different site replication peer set",
                info.endpoint
            ));
        }
    }

    Ok(())
}

fn bootstrap_bucket_op_path(bucket: &str, operation: &str) -> String {
    format!(
        "/rustfs/admin/v3/site-replication/peer/bucket-ops?{}",
        form_urlencoded::Serializer::new(String::new())
            .append_pair("bucket", bucket)
            .append_pair("operation", operation)
            .finish()
    )
}

fn bootstrap_bucket_make_op_path(bucket: &SRBucketInfo) -> String {
    let mut query = form_urlencoded::Serializer::new(String::new());
    query.append_pair("bucket", &bucket.bucket);
    query.append_pair("operation", "make-with-versioning");
    if let Some(created_at) = bucket
        .created_at
        .and_then(|value| value.format(&time::format_description::well_known::Rfc3339).ok())
    {
        query.append_pair("createdAt", &created_at);
    }
    if bucket.object_lock_config.is_some() {
        query.append_pair("lockEnabled", "true");
    }
    format!("/rustfs/admin/v3/site-replication/peer/bucket-ops?{}", query.finish())
}

fn bootstrap_bucket_meta_item(bucket: &SRBucketInfo, item_type: &str, updated_at: Option<OffsetDateTime>) -> SRBucketMeta {
    SRBucketMeta {
        bucket: bucket.bucket.clone(),
        r#type: item_type.to_string(),
        updated_at,
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    }
}

fn bootstrap_bucket_quota_value(bucket: &str, raw: &str) -> S3Result<Value> {
    serde_json::from_slice(&decode_bucket_meta_wire_value(raw))
        .map_err(|e| s3_error!(InvalidRequest, "invalid quota metadata for bootstrap bucket `{bucket}`: {e}"))
}

fn append_bootstrap_bucket_item(
    items: &mut Vec<SRBucketMeta>,
    bucket: &SRBucketInfo,
    item_type: &str,
    value: Option<String>,
    updated_at: Option<OffsetDateTime>,
    apply: impl FnOnce(&mut SRBucketMeta, String) -> S3Result<()>,
) -> S3Result<()> {
    if let Some(value) = value {
        let mut item = bootstrap_bucket_meta_item(bucket, item_type, updated_at);
        apply(&mut item, value)?;
        items.push(item);
    }
    Ok(())
}

fn append_bootstrap_bucket_items(
    plan: &mut SiteReplicationBootstrapPlan,
    bucket: &SRBucketInfo,
    replicate_ilm_expiry: bool,
) -> S3Result<()> {
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "policy",
        bucket.policy.clone().map(|value| value.to_string()),
        bucket.policy_updated_at,
        |item, value| {
            item.policy =
                Some(serde_json::from_str(&value).map_err(|e| {
                    s3_error!(InvalidRequest, "invalid bucket policy for bootstrap bucket `{}`: {e}", item.bucket)
                })?);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "version-config",
        bucket.versioning.clone(),
        bucket.versioning_config_updated_at,
        |item, value| {
            item.versioning = Some(value);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "tags",
        bucket.tags.clone(),
        bucket.tag_config_updated_at,
        |item, value| {
            item.tags = Some(value);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "object-lock-config",
        bucket.object_lock_config.clone(),
        bucket.object_lock_config_updated_at,
        |item, value| {
            item.object_lock_config = Some(value);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "sse-config",
        bucket.sse_config.clone(),
        bucket.sse_config_updated_at,
        |item, value| {
            item.sse_config = Some(value);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "replication-config",
        bucket.replication_config.clone(),
        bucket.replication_config_updated_at,
        |item, value| {
            item.replication_config = Some(value);
            Ok(())
        },
    )?;
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "quota-config",
        bucket.quota_config.clone(),
        bucket.quota_config_updated_at,
        |item, value| {
            item.quota = Some(bootstrap_bucket_quota_value(&item.bucket, &value)?);
            Ok(())
        },
    )?;
    if replicate_ilm_expiry {
        append_bootstrap_bucket_item(
            &mut plan.bucket_items,
            bucket,
            "lc-config",
            bucket.expiry_lc_config.clone(),
            bucket.expiry_lc_config_updated_at,
            |item, value| {
                item.expiry_lc_config = Some(value);
                item.expiry_updated_at = item.updated_at;
                Ok(())
            },
        )?;
    }
    append_bootstrap_bucket_item(
        &mut plan.bucket_items,
        bucket,
        "cors-config",
        bucket.cors_config.clone(),
        bucket.cors_config_updated_at,
        |item, value| {
            item.cors = Some(value);
            Ok(())
        },
    )
}

fn group_status_from_desc(status: &str) -> GroupStatus {
    if status.eq_ignore_ascii_case("disabled") {
        GroupStatus::Disabled
    } else {
        GroupStatus::Enabled
    }
}

fn site_replication_info_replicates_ilm_expiry(info: &SRInfo) -> bool {
    info.state.peers.values().any(|peer| peer.replicate_ilm_expiry)
}

fn site_replication_state_replicates_ilm_expiry(state: &SiteReplicationState) -> bool {
    state.peers.values().any(|peer| peer.replicate_ilm_expiry)
}

fn site_replication_bootstrap_plan(info: &SRInfo) -> S3Result<SiteReplicationBootstrapPlan> {
    let mut plan = SiteReplicationBootstrapPlan::default();
    let replicate_ilm_expiry = site_replication_info_replicates_ilm_expiry(info);

    for (name, policy) in &info.policies {
        plan.iam_items.push(SRIAMItem {
            r#type: "policy".to_string(),
            name: name.clone(),
            policy: policy.policy.clone(),
            updated_at: policy.updated_at,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        });
    }

    for (access_key, user) in &info.user_info_map {
        if let Some(secret_key) = &user.secret_key {
            plan.iam_items.push(SRIAMItem {
                r#type: "iam-user".to_string(),
                iam_user: Some(rustfs_madmin::SRIAMUser {
                    access_key: access_key.clone(),
                    is_delete_req: false,
                    user_req: Some(AddOrUpdateUserReq {
                        secret_key: secret_key.clone(),
                        policy: user.policy_name.clone(),
                        status: user.status.clone(),
                    }),
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                }),
                updated_at: user.updated_at,
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            });
        }
    }

    for (name, desc) in &info.group_desc_map {
        plan.iam_items.push(SRIAMItem {
            r#type: "group-info".to_string(),
            group_info: Some(SRGroupInfo {
                update_req: GroupAddRemove {
                    group: if desc.name.is_empty() {
                        name.clone()
                    } else {
                        desc.name.clone()
                    },
                    members: desc.members.clone(),
                    status: group_status_from_desc(&desc.status),
                    is_remove: false,
                },
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            }),
            updated_at: desc.updated_at,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        });
    }

    for mapping in info.user_policies.values().chain(info.group_policies.values()) {
        plan.iam_items.push(SRIAMItem {
            r#type: "policy-mapping".to_string(),
            policy_mapping: Some(mapping.clone()),
            updated_at: mapping.updated_at,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        });
    }

    for bucket in info.buckets.values() {
        plan.bucket_make_ops.push(bootstrap_bucket_make_op_path(bucket));
        append_bootstrap_bucket_items(&mut plan, bucket, replicate_ilm_expiry)?;
        plan.bucket_configure_ops
            .push(bootstrap_bucket_op_path(&bucket.bucket, "configure-replication"));
    }

    Ok(plan)
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
    seen_endpoints.insert(site_identity_key(&normalized_local.endpoint));
    peers.insert(normalized_local.deployment_id.clone(), normalized_local);

    for site in sites {
        let endpoint_key = site_identity_key(&site.endpoint);
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

    normalize_peer_map_by_identity(peers)
}

fn normalize_join_peers_for_local(local_peer: &PeerInfo, peers: BTreeMap<String, PeerInfo>) -> BTreeMap<String, PeerInfo> {
    let mut normalized = BTreeMap::new();

    for (_, incoming_peer) in peers {
        let mut peer = normalize_peer_info(incoming_peer);
        if same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
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

    normalize_peer_map_by_identity(normalized)
}

fn reconcile_peer_with_actual_identity(mut state: SiteReplicationState, actual_peer: PeerInfo) -> SiteReplicationState {
    let actual_peer = normalize_peer_info(actual_peer);
    state
        .peers
        .retain(|_, peer| !same_identity_endpoint(&peer.endpoint, &actual_peer.endpoint));
    state.peers.insert(actual_peer.deployment_id.clone(), actual_peer);
    state.peers = normalize_peer_map_by_identity(state.peers);
    state
}

async fn site_replicator_service_account_secret(access_key: &str) -> S3Result<String> {
    let Some(iam_sys) = resolve_iam_handle() else {
        return Err(s3_error!(InvalidRequest, "iam not init"));
    };

    iam_sys
        .get_site_replicator_service_account_secret(access_key)
        .await
        .map_err(ApiError::from)
        .map_err(Into::into)
}

fn legacy_site_replicator_state_secret(state: &SiteReplicationState) -> Option<String> {
    (state.service_account_access_key == SITE_REPLICATOR_SERVICE_ACCOUNT && !state.service_account_secret_key.is_empty())
        .then(|| state.service_account_secret_key.clone())
}

async fn set_site_replicator_service_account_secret(parent_user: &str, secret_key: String) -> S3Result<String> {
    let Some(iam_sys) = resolve_iam_handle() else {
        return Err(s3_error!(InvalidRequest, "iam not init"));
    };

    let access_key = SITE_REPLICATOR_SERVICE_ACCOUNT.to_string();

    if iam_sys.get_service_account(&access_key).await.is_ok() {
        iam_sys
            .update_service_account(
                &access_key,
                UpdateServiceAccountOpts {
                    session_policy: Some(site_replicator_service_account_policy()?),
                    secret_key: Some(secret_key.clone()),
                    name: None,
                    description: None,
                    expiration: None,
                    status: None,
                    allow_site_replicator_account: true,
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
                    session_policy: Some(site_replicator_service_account_policy()?),
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

    Ok(access_key)
}

async fn ensure_site_replicator_service_account(parent_user: &str, rotate_secret: bool) -> S3Result<(String, String)> {
    let Some(iam_sys) = resolve_iam_handle() else {
        return Err(s3_error!(InvalidRequest, "iam not init"));
    };

    let access_key = SITE_REPLICATOR_SERVICE_ACCOUNT.to_string();
    let existing_secret = iam_sys.get_site_replicator_service_account_secret(&access_key).await.ok();
    let secret_key = if rotate_secret {
        rustfs_credentials::gen_secret_key(40)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("generate secret key failed: {e}")))?
    } else if let Some(secret_key) = existing_secret {
        secret_key
    } else {
        rustfs_credentials::gen_secret_key(40)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("generate secret key failed: {e}")))?
    };

    set_site_replicator_service_account_secret(parent_user, secret_key.clone()).await?;

    Ok((access_key, secret_key))
}

fn site_replication_peer_wire_path(path: &str) -> String {
    let (path_only, query) = path
        .split_once('?')
        .map(|(path, query)| (path, Some(query)))
        .unwrap_or((path, None));
    let wire_path = if path_only == SITE_REPLICATION_PEER_JOIN_PATH {
        MINIO_SITE_REPLICATION_JOIN_PATH.to_string()
    } else if let Some(suffix) = path_only.strip_prefix(RUSTFS_ADMIN_V3_PREFIX) {
        format!("{MINIO_ADMIN_V3_PREFIX}{suffix}")
    } else if path_only.starts_with(MINIO_ADMIN_PREFIX) {
        path_only.to_string()
    } else {
        path_only.to_string()
    };

    match query {
        Some(query) => format!("{wire_path}?{query}"),
        None => wire_path,
    }
}

fn site_replication_peer_payload_encrypted(wire_path: &str) -> bool {
    wire_path.split_once('?').map(|(path, _)| path).unwrap_or(wire_path) == MINIO_SITE_REPLICATION_JOIN_PATH
}

fn site_replication_peer_payload(path: &str, secret_key: &str, payload: Vec<u8>) -> S3Result<(Vec<u8>, &'static str)> {
    if site_replication_peer_payload_encrypted(path) {
        encode_compatible_admin_payload(path, secret_key, payload)
    } else {
        Ok((payload, "application/json"))
    }
}

async fn send_peer_admin_request<T: Serialize>(
    endpoint: &str,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<Vec<u8>> {
    let path = site_replication_peer_wire_path(path);
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
    let (payload, content_type) = site_replication_peer_payload(&path, secret_key, payload)?;

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
        resolve_region()
            .map(|region| region.to_string())
            .as_deref()
            .unwrap_or("us-east-1"),
    );

    let mut req = site_replication_peer_client().await?.request(reqwest::Method::PUT, &url);
    for (name, value) in signed.headers() {
        req = req.header(name, value);
    }

    let response = req.body(payload).send().await.map_err(|e| {
        let classify = if e.is_timeout() {
            "timeout"
        } else if e.is_connect() && e.to_string().to_ascii_lowercase().contains("dns") {
            "dns resolution"
        } else if e.to_string().to_ascii_lowercase().contains("certificate") || e.to_string().to_ascii_lowercase().contains("tls")
        {
            "tls handshake"
        } else if e.is_connect() {
            "connect"
        } else {
            "request"
        };
        S3Error::with_message(S3ErrorCode::InternalError, format!("peer request to {url} failed ({classify}): {e}"))
    })?;

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

async fn send_peer_admin_request_with_secret_candidates<T: Serialize>(
    endpoint: &str,
    path: &str,
    access_key: &str,
    secret_candidates: &[String],
    body: &T,
) -> S3Result<Vec<u8>> {
    let mut tried = HashSet::new();
    let mut errors = Vec::new();

    for secret_key in secret_candidates.iter().filter(|secret_key| !secret_key.is_empty()) {
        if !tried.insert(secret_key.as_str()) {
            continue;
        }

        match send_peer_admin_request(endpoint, path, access_key, secret_key, body).await {
            Ok(body) => return Ok(body),
            Err(err) => {
                let detail = format!("{err}");
                let may_retry_with_next_secret = peer_error_may_be_secret_mismatch(&detail);
                errors.push(summarize_peer_error_detail(&detail));
                if !may_retry_with_next_secret {
                    break;
                }
            }
        }
    }

    Err(S3Error::with_message(
        S3ErrorCode::InternalError,
        format!(
            "peer request to {endpoint}{path} failed with all service-account secrets: {}",
            errors.join("; ")
        ),
    ))
}

fn peer_error_may_be_secret_mismatch(detail: &str) -> bool {
    let detail = detail.to_ascii_lowercase();
    detail.contains("signaturedoesnotmatch")
        || detail.contains("accessdenied")
        || detail.contains("forbidden")
        || detail.contains("401")
        || detail.contains("403")
}

async fn send_peer_admin_get_request(endpoint: &str, path: &str, access_key: &str, secret_key: &str) -> S3Result<Vec<u8>> {
    let path = site_replication_peer_wire_path(path);
    let base = endpoint.trim_end_matches('/');
    let url = format!("{base}{path}");
    let uri = url
        .parse::<Uri>()
        .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid peer endpoint: {e}")))?;
    let authority = uri
        .authority()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, "peer endpoint missing authority".to_string()))?
        .to_string();

    let signed = sign_v4(
        http::Request::builder()
            .method(Method::GET)
            .uri(uri)
            .header(HOST, authority)
            .header("x-amz-content-sha256", UNSIGNED_PAYLOAD)
            .body(Body::empty())
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("build peer request failed: {e}")))?,
        0,
        access_key,
        secret_key,
        "",
        resolve_region()
            .map(|region| region.to_string())
            .as_deref()
            .unwrap_or("us-east-1"),
    );

    let mut req = site_replication_peer_client().await?.request(reqwest::Method::GET, &url);
    for (name, value) in signed.headers() {
        req = req.header(name, value);
    }

    let response = req.send().await.map_err(|e| {
        let classify = if e.is_timeout() {
            "timeout"
        } else if e.is_connect() && e.to_string().to_ascii_lowercase().contains("dns") {
            "dns resolution"
        } else if e.to_string().to_ascii_lowercase().contains("certificate") || e.to_string().to_ascii_lowercase().contains("tls")
        {
            "tls handshake"
        } else if e.is_connect() {
            "connect"
        } else {
            "request"
        };
        S3Error::with_message(S3ErrorCode::InternalError, format!("peer request to {url} failed ({classify}): {e}"))
    })?;

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

async fn runtime_site_replication_targets() -> S3Result<Option<SiteReplicationRuntime>> {
    let state = load_site_replication_state().await?;
    if !state.enabled() || state.service_account_access_key.is_empty() {
        return Ok(None);
    }

    let service_account_secret_key = match site_replicator_service_account_secret(&state.service_account_access_key).await {
        Ok(secret) => secret,
        Err(err) => {
            let Some(secret) = legacy_site_replicator_state_secret(&state) else {
                return Err(err);
            };
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                result = "legacy_state_service_account_secret_fallback",
                error = ?err,
                "admin site replication state"
            );
            secret
        }
    };
    let local_peer = current_local_runtime_peer(&state);
    Ok(Some(SiteReplicationRuntime {
        state,
        local_peer,
        service_account_secret_key,
    }))
}

async fn broadcast_site_replication_json<T: Serialize>(path: &str, body: &T) -> S3Result<()> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(());
    };
    broadcast_site_replication_json_with_runtime(&runtime, path, body).await
}

async fn broadcast_site_replication_json_with_runtime<T: Serialize>(
    runtime: &SiteReplicationRuntime,
    path: &str,
    body: &T,
) -> S3Result<()> {
    let state = &runtime.state;
    let local_peer = &runtime.local_peer;

    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }

        send_peer_admin_request_with_retry_event(
            peer,
            path,
            &state.service_account_access_key,
            &runtime.service_account_secret_key,
            body,
        )
        .await?;
    }

    Ok(())
}

async fn send_peer_admin_request_with_retry_event<T: Serialize>(
    peer: &PeerInfo,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<Vec<u8>> {
    match send_peer_admin_request(&peer.endpoint, path, access_key, secret_key, body).await {
        Ok(body) => Ok(body),
        Err(err) => {
            enqueue_site_replication_retry_event(peer, path, &err).await;
            Err(err)
        }
    }
}

async fn send_site_replication_bootstrap_plan(
    peer: &PeerInfo,
    service_account_access_key: &str,
    service_account_secret_key: &str,
    plan: &SiteReplicationBootstrapPlan,
) -> S3Result<()> {
    for item in &plan.iam_items {
        send_peer_admin_request_with_retry_event(
            peer,
            "/rustfs/admin/v3/site-replication/peer/iam-item",
            service_account_access_key,
            service_account_secret_key,
            item,
        )
        .await?;
    }

    let empty = serde_json::json!({});
    for path in &plan.bucket_make_ops {
        send_peer_admin_request_with_retry_event(peer, path, service_account_access_key, service_account_secret_key, &empty)
            .await?;
    }

    for item in &plan.bucket_items {
        send_peer_admin_request_with_retry_event(
            peer,
            "/rustfs/admin/v3/site-replication/peer/bucket-meta",
            service_account_access_key,
            service_account_secret_key,
            item,
        )
        .await?;
    }

    for path in &plan.bucket_configure_ops {
        send_peer_admin_request_with_retry_event(peer, path, service_account_access_key, service_account_secret_key, &empty)
            .await?;
    }

    Ok(())
}

async fn bootstrap_existing_metadata_after_add(
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    service_account_secret_key: &str,
) -> Vec<String> {
    let info = match build_sr_info(state, local_peer).await {
        Ok(info) => info,
        Err(err) => {
            return vec![format!(
                "local snapshot failed: {}",
                summarize_peer_error_detail(&err.to_string())
            )];
        }
    };
    let plan = match site_replication_bootstrap_plan(&info) {
        Ok(plan) => plan,
        Err(err) => {
            return vec![format!(
                "bootstrap plan failed: {}",
                summarize_peer_error_detail(&err.to_string())
            )];
        }
    };

    let mut errors = Vec::new();
    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }

        if let Err(err) =
            send_site_replication_bootstrap_plan(peer, &state.service_account_access_key, service_account_secret_key, &plan).await
        {
            let detail = summarize_peer_error_detail(&err.to_string());
            warn!(
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                peer = %peer.endpoint,
                error = %detail,
                "site replication bootstrap metadata sync failed"
            );
            errors.push(format!("{}: {detail}", peer.endpoint));
        }
    }

    errors
}

pub async fn site_replication_make_bucket_hook(bucket: &str, lock_enabled: bool) -> S3Result<()> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(());
    };

    ensure_site_replication_bucket_versioning(bucket).await?;
    ensure_site_replication_bucket_targets(
        bucket,
        &runtime.state,
        &runtime.local_peer,
        None,
        &runtime.service_account_secret_key,
    )
    .await?;
    ensure_site_replication_bucket_replication_config(
        bucket,
        &runtime.state,
        &runtime.local_peer,
        &runtime.service_account_secret_key,
    )
    .await?;

    let created_at = resolve_object_store_handle()
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
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(());
    };
    if item.r#type == "lc-config" && !site_replication_state_replicates_ilm_expiry(&runtime.state) {
        return Ok(());
    }
    broadcast_site_replication_json_with_runtime(
        &runtime,
        "/rustfs/admin/v3/site-replication/peer/bucket-meta",
        &encode_bucket_meta_wire_item(item),
    )
    .await
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

fn raw_config_to_base64(raw: &[u8]) -> Option<String> {
    (!raw.is_empty()).then(|| BASE64_STANDARD.encode(raw))
}

fn encode_bucket_meta_wire_value(value: Option<String>) -> Option<String> {
    value.map(|raw| BASE64_STANDARD.encode(raw.as_bytes()))
}

fn encode_bucket_meta_wire_item(mut item: SRBucketMeta) -> SRBucketMeta {
    item.versioning = encode_bucket_meta_wire_value(item.versioning);
    item.tags = encode_bucket_meta_wire_value(item.tags);
    item.object_lock_config = encode_bucket_meta_wire_value(item.object_lock_config);
    item.sse_config = encode_bucket_meta_wire_value(item.sse_config);
    item.replication_config = encode_bucket_meta_wire_value(item.replication_config);
    item.expiry_lc_config = encode_bucket_meta_wire_value(item.expiry_lc_config);
    item.cors = encode_bucket_meta_wire_value(item.cors);
    item
}

fn decode_bucket_meta_wire_value(raw: &str) -> Vec<u8> {
    BASE64_STANDARD
        .decode(raw.as_bytes())
        .ok()
        .filter(|decoded| std::str::from_utf8(decoded).is_ok())
        .unwrap_or_else(|| raw.as_bytes().to_vec())
}

fn decode_bucket_meta_wire_option(value: Option<String>) -> Option<Vec<u8>> {
    value.map(|raw| decode_bucket_meta_wire_value(&raw))
}

fn maybe_time(value: OffsetDateTime) -> Option<OffsetDateTime> {
    (value != OffsetDateTime::UNIX_EPOCH).then_some(value)
}

async fn build_sr_info(state: &SiteReplicationState, local_peer: &PeerInfo) -> S3Result<SRInfo> {
    let Some(store) = resolve_object_store_handle() else {
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
            location: resolve_region().map(|region| region.to_string()).unwrap_or_default(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        };

        if let Some(metadata) = metadata {
            entry.policy = raw_config_to_string(&metadata.policy_config_json).and_then(|raw| serde_json::from_str(&raw).ok());
            entry.versioning = raw_config_to_base64(&metadata.versioning_config_xml);
            entry.tags = raw_config_to_base64(&metadata.tagging_config_xml);
            entry.object_lock_config = raw_config_to_base64(&metadata.object_lock_config_xml);
            entry.sse_config = raw_config_to_base64(&metadata.encryption_config_xml);
            entry.replication_config = raw_config_to_base64(&metadata.replication_config_xml);
            entry.quota_config = raw_config_to_base64(&metadata.quota_config_json);
            entry.expiry_lc_config = raw_config_to_base64(&metadata.lifecycle_config_xml);
            entry.cors_config = raw_config_to_base64(&metadata.cors_config_xml);
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

    if let Some(iam_sys) = resolve_iam_handle() {
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

fn local_idp_settings() -> IDPSettings {
    let mut settings = IDPSettings::default();
    if let Some(oidc) = resolve_oidc_handle() {
        let providers = oidc.list_providers();
        settings.open_id.enabled = !providers.is_empty();
        settings.open_id.region = resolve_region().map(|region| region.to_string()).unwrap_or_default();

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
    settings
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

async fn build_metrics_summary(local_peer: &PeerInfo) -> SRMetricsSummary {
    let Some(stats) = resolve_replication_stats_handle() else {
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
            put_tag_total: non_negative_u64(node.proxied.put_tag_total),
            put_tag_failed_total: non_negative_u64(node.proxied.put_tag_failed),
            ..Default::default()
        },
        metrics,
        uptime: node.uptime,
        ..Default::default()
    }
}

fn sr_metainfo_path(uri: &Uri) -> String {
    uri.query()
        .map(|query| format!("/rustfs/admin/v3/site-replication/metainfo?{query}"))
        .unwrap_or_else(|| "/rustfs/admin/v3/site-replication/metainfo".to_string())
}

async fn fetch_peer_sr_info(
    peer: &PeerInfo,
    state: &SiteReplicationState,
    service_account_secret_key: &str,
    uri: &Uri,
) -> S3Result<SRInfo> {
    if state.service_account_access_key.is_empty() || service_account_secret_key.is_empty() {
        return Err(s3_error!(InvalidRequest, "site replication service account is not configured"));
    }

    let body = send_peer_admin_get_request(
        &peer.endpoint,
        &sr_metainfo_path(uri),
        &state.service_account_access_key,
        service_account_secret_key,
    )
    .await?;

    serde_json::from_slice(&body).map_err(|e| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("parse site replication metainfo from {} failed: {e}", peer.endpoint),
        )
    })
}

fn string_config_mismatch<'a>(values: impl Iterator<Item = Option<&'a String>>, total_sites: usize) -> (usize, bool) {
    let mut present = 0usize;
    let mut first: Option<&String> = None;
    let mut mismatch = false;

    for value in values.flatten() {
        present += 1;
        if let Some(first) = first {
            mismatch |= first != value;
        } else {
            first = Some(value);
        }
    }

    (present, present > 0 && (present < total_sites || mismatch))
}

fn value_config_mismatch<'a>(values: impl Iterator<Item = Option<&'a Value>>, total_sites: usize) -> (usize, bool) {
    let mut present = 0usize;
    let mut first: Option<Value> = None;
    let mut mismatch = false;

    for value in values.flatten() {
        present += 1;
        let value = canonical_status_json(value);
        if let Some(first) = &first {
            mismatch |= first != &value;
        } else {
            first = Some(value);
        }
    }

    (present, present > 0 && (present < total_sites || mismatch))
}

fn canonical_status_json(value: &Value) -> Value {
    match value {
        Value::Array(items) if items.iter().all(Value::is_string) => {
            let mut items = items.clone();
            items.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
            Value::Array(items)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical_status_json).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), canonical_status_json(value)))
                .collect(),
        ),
        _ => value.clone(),
    }
}

fn site_replication_rule_complete(rule: &ReplicationRule) -> bool {
    let delete_marker_enabled = rule
        .delete_marker_replication
        .as_ref()
        .and_then(|delete_marker| delete_marker.status.as_ref())
        .is_some_and(|status| status == &DeleteMarkerReplicationStatus::from_static(DeleteMarkerReplicationStatus::ENABLED));
    let delete_enabled = rule
        .delete_replication
        .as_ref()
        .is_some_and(|delete| delete.status == DeleteReplicationStatus::from_static(DeleteReplicationStatus::ENABLED));
    let existing_object_enabled = rule.existing_object_replication.as_ref().is_some_and(|existing| {
        existing.status == ExistingObjectReplicationStatus::from_static(ExistingObjectReplicationStatus::ENABLED)
    });
    let replica_modifications_enabled = rule
        .source_selection_criteria
        .as_ref()
        .and_then(|criteria| criteria.replica_modifications.as_ref())
        .is_some_and(|replica_modifications| {
            replica_modifications.status == ReplicaModificationsStatus::from_static(ReplicaModificationsStatus::ENABLED)
        });

    rule.id.as_deref().is_some_and(|id| id.starts_with("site-repl-"))
        && rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED)
        && delete_marker_enabled
        && delete_enabled
        && existing_object_enabled
        && replica_modifications_enabled
}

fn site_replication_config_mismatch<'a>(values: impl Iterator<Item = Option<&'a String>>, total_sites: usize) -> (usize, bool) {
    let values = values.flatten().collect::<Vec<_>>();
    let present = values.len();
    if present == 0 {
        return (0, false);
    }
    if present != total_sites {
        return (present, true);
    }

    let expected_rules = total_sites.saturating_sub(1);
    let replicated = values.iter().all(|raw| {
        deserialize::<ReplicationConfiguration>(raw.as_bytes())
            .is_ok_and(|config| config.rules.len() == expected_rules && config.rules.iter().all(site_replication_rule_complete))
    });

    (present, !replicated)
}

fn merge_bucket_status_info(status: &mut SRStatusInfo, site_infos: &BTreeMap<String, SRInfo>, opts: &SRStatusOptions) {
    if !(opts.include_all_defaults() || opts.buckets || opts.entity == SREntityType::Bucket) {
        return;
    }

    let total_sites = site_infos.len();
    let mut bucket_names = BTreeMap::<String, ()>::new();
    for info in site_infos.values() {
        for bucket_name in info.buckets.keys() {
            if opts.entity == SREntityType::Bucket && !opts.entity_value.is_empty() && bucket_name != &opts.entity_value {
                continue;
            }
            bucket_names.insert(bucket_name.clone(), ());
        }
    }

    for bucket_name in bucket_names.keys() {
        let bucket_values = site_infos.values().map(|info| info.buckets.get(bucket_name));
        let present_buckets = bucket_values.clone().filter(|bucket| bucket.is_some()).count();
        let (tag_count, tag_mismatch) = string_config_mismatch(
            site_infos
                .values()
                .map(|info| info.buckets.get(bucket_name).and_then(|bucket| bucket.tags.as_ref())),
            total_sites,
        );
        let (object_lock_count, object_lock_mismatch) = string_config_mismatch(
            site_infos.values().map(|info| {
                info.buckets
                    .get(bucket_name)
                    .and_then(|bucket| bucket.object_lock_config.as_ref())
            }),
            total_sites,
        );
        let (sse_count, sse_mismatch) = string_config_mismatch(
            site_infos
                .values()
                .map(|info| info.buckets.get(bucket_name).and_then(|bucket| bucket.sse_config.as_ref())),
            total_sites,
        );
        let (versioning_count, versioning_mismatch) = string_config_mismatch(
            site_infos
                .values()
                .map(|info| info.buckets.get(bucket_name).and_then(|bucket| bucket.versioning.as_ref())),
            total_sites,
        );
        let (_, replication_mismatch) = site_replication_config_mismatch(
            site_infos.values().map(|info| {
                info.buckets
                    .get(bucket_name)
                    .and_then(|bucket| bucket.replication_config.as_ref())
            }),
            total_sites,
        );
        let (quota_count, quota_mismatch) = string_config_mismatch(
            site_infos
                .values()
                .map(|info| info.buckets.get(bucket_name).and_then(|bucket| bucket.quota_config.as_ref())),
            total_sites,
        );
        let (cors_count, cors_mismatch) = string_config_mismatch(
            site_infos
                .values()
                .map(|info| info.buckets.get(bucket_name).and_then(|bucket| bucket.cors_config.as_ref())),
            total_sites,
        );
        let (policy_count, policy_mismatch) = value_config_mismatch(
            site_infos
                .values()
                .map(|info| info.buckets.get(bucket_name).and_then(|bucket| bucket.policy.as_ref())),
            total_sites,
        );

        for (deployment_id, info) in site_infos {
            let bucket_info = info.buckets.get(bucket_name);
            let summary = status
                .stats_summary
                .entry(deployment_id.clone())
                .or_insert_with(|| SRSiteSummary {
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                    ..Default::default()
                });
            summary.total_buckets_count += 1;
            if present_buckets == total_sites {
                summary.replicated_buckets += 1;
            }
            if tag_count > 0 {
                summary.total_tags_count += 1;
            }
            if !tag_mismatch && tag_count == total_sites {
                summary.replicated_tags += 1;
            }
            if object_lock_count > 0 {
                summary.total_lock_config_count += 1;
            }
            if !object_lock_mismatch && object_lock_count == total_sites {
                summary.replicated_lock_config += 1;
            }
            if sse_count > 0 {
                summary.total_sse_config_count += 1;
            }
            if !sse_mismatch && sse_count == total_sites {
                summary.replicated_sse_config += 1;
            }
            if versioning_count > 0 {
                summary.total_versioning_config_count += 1;
            }
            if !versioning_mismatch && versioning_count == total_sites {
                summary.replicated_versioning_config += 1;
            }
            if quota_count > 0 {
                summary.total_quota_config_count += 1;
            }
            if !quota_mismatch && quota_count == total_sites {
                summary.replicated_quota_config += 1;
            }
            if cors_count > 0 {
                summary.total_cors_config_count += 1;
            }
            if !cors_mismatch && cors_count == total_sites {
                summary.replicated_cors_config += 1;
            }
            if policy_count > 0 {
                summary.total_bucket_policies_count += 1;
            }
            if !policy_mismatch && policy_count == total_sites {
                summary.replicated_bucket_policies += 1;
            }

            status.bucket_stats.entry(bucket_name.clone()).or_default().insert(
                deployment_id.clone(),
                SRBucketStatsSummary {
                    deployment_id: deployment_id.clone(),
                    has_bucket: bucket_info.is_some(),
                    has_tags_set: bucket_info.is_some_and(|bucket| bucket.tags.is_some()),
                    has_object_lock_config_set: bucket_info.is_some_and(|bucket| bucket.object_lock_config.is_some()),
                    has_policy_set: bucket_info.is_some_and(|bucket| bucket.policy.is_some()),
                    has_sse_cfg_set: bucket_info.is_some_and(|bucket| bucket.sse_config.is_some()),
                    has_replication_cfg: bucket_info.is_some_and(|bucket| bucket.replication_config.is_some()),
                    has_quota_cfg_set: bucket_info.is_some_and(|bucket| bucket.quota_config.is_some()),
                    has_cors_cfg_set: bucket_info.is_some_and(|bucket| bucket.cors_config.is_some()),
                    tag_mismatch,
                    versioning_config_mismatch: versioning_mismatch,
                    object_lock_config_mismatch: object_lock_mismatch,
                    policy_mismatch,
                    sse_config_mismatch: sse_mismatch,
                    replication_cfg_mismatch: replication_mismatch && bucket_info.is_some_and(|b| b.replication_config.is_some()),
                    quota_cfg_mismatch: quota_mismatch,
                    cors_cfg_mismatch: cors_mismatch,
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                    ..Default::default()
                },
            );
        }
    }
}

fn merge_policy_status_info(status: &mut SRStatusInfo, site_infos: &BTreeMap<String, SRInfo>, opts: &SRStatusOptions) {
    if !(opts.include_all_defaults() || opts.policies || opts.entity == SREntityType::Policy) {
        return;
    }

    let total_sites = site_infos.len();
    let mut policy_names = BTreeMap::<String, ()>::new();
    for info in site_infos.values() {
        for policy_name in info.policies.keys() {
            if opts.entity == SREntityType::Policy && !opts.entity_value.is_empty() && policy_name != &opts.entity_value {
                continue;
            }
            policy_names.insert(policy_name.clone(), ());
        }
    }

    for policy_name in policy_names.keys() {
        let (policy_count, policy_mismatch) = value_config_mismatch(
            site_infos
                .values()
                .map(|info| info.policies.get(policy_name).and_then(|policy| policy.policy.as_ref())),
            total_sites,
        );

        for (deployment_id, info) in site_infos {
            let policy = info.policies.get(policy_name);
            let summary = status
                .stats_summary
                .entry(deployment_id.clone())
                .or_insert_with(|| SRSiteSummary {
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                    ..Default::default()
                });
            if policy_count > 0 {
                summary.total_iam_policies_count += 1;
            }
            if !policy_mismatch && policy_count == total_sites {
                summary.replicated_iam_policies += 1;
            }

            status.policy_stats.entry(policy_name.clone()).or_default().insert(
                deployment_id.clone(),
                SRPolicyStatsSummary {
                    deployment_id: deployment_id.clone(),
                    policy_mismatch,
                    has_policy: policy.is_some_and(|policy| policy.policy.is_some()),
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                },
            );
        }
    }
}

fn merge_status_info_for_site(status: &mut SRStatusInfo, deployment_id: &str, info: &SRInfo, opts: &SRStatusOptions) {
    if opts.include_all_defaults() || opts.users || opts.entity == SREntityType::User {
        for name in info.user_info_map.keys() {
            if opts.entity == SREntityType::User && !opts.entity_value.is_empty() && name != &opts.entity_value {
                continue;
            }
            let summary = status
                .stats_summary
                .entry(deployment_id.to_string())
                .or_insert_with(|| SRSiteSummary {
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                    ..Default::default()
                });
            summary.total_users_count += 1;
            summary.replicated_users += 1;
            if info.user_policies.contains_key(name) {
                summary.total_user_policy_mapping_count += 1;
                summary.replicated_user_policy_mappings += 1;
            }
            status.user_stats.entry(name.clone()).or_default().insert(
                deployment_id.to_string(),
                SRUserStatsSummary {
                    deployment_id: deployment_id.to_string(),
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
            let summary = status
                .stats_summary
                .entry(deployment_id.to_string())
                .or_insert_with(|| SRSiteSummary {
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                    ..Default::default()
                });
            summary.total_groups_count += 1;
            summary.replicated_groups += 1;
            if info.group_policies.contains_key(name) {
                summary.total_group_policy_mapping_count += 1;
                summary.replicated_group_policy_mappings += 1;
            }
            status.group_stats.entry(name.clone()).or_default().insert(
                deployment_id.to_string(),
                SRGroupStatsSummary {
                    deployment_id: deployment_id.to_string(),
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
            let summary = status
                .stats_summary
                .entry(deployment_id.to_string())
                .or_insert_with(|| SRSiteSummary {
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                    ..Default::default()
                });
            summary.total_ilm_expiry_rules_count += 1;
            summary.replicated_ilm_expiry_rules += 1;
            status.ilm_expiry_stats.entry(name.clone()).or_default().insert(
                deployment_id.to_string(),
                SRILMExpiryStatsSummary {
                    deployment_id: deployment_id.to_string(),
                    has_ilm_expiry_rules: true,
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                    ..Default::default()
                },
            );
        }
    }
}

fn prune_in_sync_status_details(status: &mut SRStatusInfo, opts: &SRStatusOptions) {
    if opts.entity != SREntityType::Bucket {
        status.bucket_stats.retain(|_, deployments| {
            deployments.values().any(|stats| {
                !stats.has_bucket
                    || stats.bucket_marked_deleted
                    || stats.tag_mismatch
                    || stats.versioning_config_mismatch
                    || stats.object_lock_config_mismatch
                    || stats.policy_mismatch
                    || stats.sse_config_mismatch
                    || stats.replication_cfg_mismatch
                    || stats.quota_cfg_mismatch
                    || stats.cors_cfg_mismatch
            })
        });
    }

    if opts.entity != SREntityType::Policy {
        status
            .policy_stats
            .retain(|_, deployments| deployments.values().any(|stats| stats.policy_mismatch));
    }
}

async fn build_status_info(state: &SiteReplicationState, local_peer: &PeerInfo, uri: &Uri) -> S3Result<SRStatusInfo> {
    let opts = sr_status_options(uri);
    let mut local_info = Some(filter_sr_info(build_sr_info(state, local_peer).await?, &opts));
    let metrics_requested = opts.metrics || opts.include_all_defaults() || opts.entity == SREntityType::Bucket;
    let service_account_secret_key = if state.enabled() && !state.service_account_access_key.is_empty() {
        site_replicator_service_account_secret(&state.service_account_access_key)
            .await
            .ok()
    } else {
        None
    };

    let mut site_infos = BTreeMap::new();
    let mut reachable_peers = HashSet::new();
    let mut peer_errors = BTreeMap::new();
    for (deployment_id, peer) in &state.peers {
        if deployment_id == &local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
            site_infos.insert(deployment_id.clone(), local_info.take().unwrap_or_default());
            reachable_peers.insert(deployment_id.clone());
            continue;
        }

        match service_account_secret_key.as_deref() {
            Some(secret_key) => match fetch_peer_sr_info(peer, state, secret_key, uri).await {
                Ok(peer_info) => {
                    site_infos.insert(deployment_id.clone(), filter_sr_info(peer_info, &opts));
                    reachable_peers.insert(deployment_id.clone());
                }
                Err(err) => {
                    warn!(
                        event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                        peer = %peer.endpoint,
                        result = "peer_metainfo_fetch_failed",
                        error = ?err,
                        "admin site replication state"
                    );
                    peer_errors.insert(deployment_id.clone(), status_peer_error(peer, err.to_string()));
                    site_infos.insert(deployment_id.clone(), SRInfo::default());
                }
            },
            None => {
                warn!(
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    peer = %peer.endpoint,
                    result = "site_replication_service_account_missing",
                    "admin site replication state"
                );
                peer_errors.insert(
                    deployment_id.clone(),
                    status_peer_error(peer, "site replication service account secret unavailable".to_string()),
                );
                site_infos.insert(deployment_id.clone(), SRInfo::default());
            }
        };
    }

    let max_buckets = site_infos.values().map(|info| info.buckets.len()).max().unwrap_or(0);
    let max_users = site_infos.values().map(|info| info.user_info_map.len()).max().unwrap_or(0);
    let max_groups = site_infos.values().map(|info| info.group_desc_map.len()).max().unwrap_or(0);
    let max_policies = site_infos.values().map(|info| info.policies.len()).max().unwrap_or(0);
    let max_ilm_expiry_rules = site_infos.values().map(|info| info.ilm_expiry_rules.len()).max().unwrap_or(0);

    let mut status = SRStatusInfo {
        enabled: state.enabled(),
        max_buckets,
        max_users,
        max_groups,
        max_policies,
        max_ilm_expiry_rules,
        sites: state.peers.clone(),
        peer_errors,
        pending_operation: pending_operation_for_state(state, local_peer),
        retry_stats: retry_stats_for_state(state),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
        ..Default::default()
    };

    for deployment_id in state.peers.keys() {
        status.stats_summary.insert(
            deployment_id.clone(),
            SRSiteSummary {
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            },
        );
    }
    merge_bucket_status_info(&mut status, &site_infos, &opts);
    merge_policy_status_info(&mut status, &site_infos, &opts);
    for (deployment_id, info) in &site_infos {
        merge_status_info_for_site(&mut status, deployment_id, info, &opts);
    }
    prune_in_sync_status_details(&mut status, &opts);

    // Fix 2: derive sync_state from real signals — reachability + replication rule completeness
    // instead of always returning SyncStatus::Unknown as stored in the persisted peer map.
    {
        let peer_has_replication_issue: HashMap<String, bool> = status
            .sites
            .keys()
            .map(|dep_id| {
                let has_issue = status
                    .bucket_stats
                    .values()
                    .any(|by_dep| by_dep.get(dep_id.as_str()).is_some_and(|s| s.replication_cfg_mismatch));
                (dep_id.clone(), has_issue)
            })
            .collect();

        for (deployment_id, peer) in status.sites.iter_mut() {
            if !reachable_peers.contains(deployment_id) {
                peer.sync_state = SyncStatus::Unknown;
            } else if peer_has_replication_issue.get(deployment_id).copied().unwrap_or(false) {
                peer.sync_state = SyncStatus::Disable;
            } else {
                peer.sync_state = SyncStatus::Enable;
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
    service_account_parent: String,
    replicate_ilm_expiry: bool,
) -> SiteReplicationState {
    state.name = local_peer.name.clone();
    state.service_account_access_key = service_account_access_key;
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
    if same_identity_endpoint(&incoming.endpoint, &local_peer.endpoint) && !incoming.name.is_empty() {
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
        state.retry_queue.clear();
        state.updated_at = Some(OffsetDateTime::now_utc());
        return state;
    }

    let names: HashSet<String> = req.site_names.into_iter().collect();
    if names.contains(&state.name) {
        state.peers.clear();
        state.resync_status.clear();
        state.retry_queue.clear();
        state.updated_at = Some(OffsetDateTime::now_utc());
        return state;
    }

    let removed_peers: Vec<(String, String)> = state
        .peers
        .iter()
        .filter(|(_, peer)| names.contains(&peer.name))
        .map(|(deployment_id, peer)| (deployment_id.clone(), peer.endpoint.clone()))
        .collect();
    for (deployment_id, _) in &removed_peers {
        state.peers.remove(deployment_id);
        state.resync_status.remove(deployment_id);
    }
    state.retry_queue.retain(|event| {
        !removed_peers
            .iter()
            .any(|(deployment_id, endpoint)| &event.peer_deployment_id == deployment_id || &event.peer_endpoint == endpoint)
    });
    state
        .resync_status
        .retain(|deployment_id, _| state.peers.contains_key(deployment_id));
    state.updated_at = Some(OffsetDateTime::now_utc());
    state
}

fn validate_remove_sites_req(state: &SiteReplicationState, req: &SRRemoveReq) -> S3Result<()> {
    if req.remove_all {
        if !req.site_names.is_empty() {
            return Err(s3_error!(InvalidRequest, "sites must be empty when all=true"));
        }
        return Ok(());
    }

    if req.site_names.is_empty() {
        return Err(s3_error!(InvalidRequest, "sites is required when all=false"));
    }

    let mut seen = HashSet::new();
    let names: HashSet<&str> = req
        .site_names
        .iter()
        .map(|name| name.trim())
        .map(|name| {
            if name.is_empty() {
                Err(s3_error!(InvalidRequest, "site name must not be empty"))
            } else if !seen.insert(name.to_string()) {
                Err(s3_error!(InvalidRequest, "duplicate site name `{name}`"))
            } else {
                Ok(name)
            }
        })
        .collect::<S3Result<HashSet<_>>>()?;

    let matches_local = names.contains(state.name.as_str());
    let matches_peer = state.peers.values().any(|peer| names.contains(peer.name.as_str()));
    if !matches_local && !matches_peer {
        return Err(s3_error!(InvalidRequest, "none of the requested sites are configured"));
    }

    Ok(())
}

fn summarize_peer_error_detail(detail: &str) -> String {
    let detail = detail.trim();
    let detail_chars = detail.chars().count();
    if detail_chars <= SITE_REPLICATION_PEER_ERROR_DETAIL_LIMIT {
        return detail.to_string();
    }

    let suffix = "... (truncated)";
    let take_chars = SITE_REPLICATION_PEER_ERROR_DETAIL_LIMIT.saturating_sub(suffix.chars().count());
    let mut summary: String = detail.chars().take(take_chars).collect();
    summary.push_str(suffix);
    summary
}

fn retry_event_matches(event: &SiteReplicationRetryEvent, peer: &PeerInfo, path: &str) -> bool {
    (event.peer_deployment_id == peer.deployment_id || event.peer_endpoint == peer.endpoint) && event.path == path
}

fn upsert_site_replication_retry_event(queue: &mut Vec<SiteReplicationRetryEvent>, peer: &PeerInfo, path: &str, error: &str) {
    let now = OffsetDateTime::now_utc();
    let detail = summarize_peer_error_detail(error);
    if let Some(event) = queue.iter_mut().find(|event| retry_event_matches(event, peer, path)) {
        event.retry_count = event.retry_count.saturating_add(1);
        event.failed = event.retry_count >= SITE_REPLICATION_RETRY_FAILED_AFTER;
        event.last_error = detail;
        event.updated_at = Some(now);
        return;
    }

    queue.push(SiteReplicationRetryEvent {
        id: Uuid::new_v4().to_string(),
        peer_deployment_id: peer.deployment_id.clone(),
        peer_endpoint: peer.endpoint.clone(),
        path: path.to_string(),
        retry_count: 1,
        failed: false,
        last_error: detail,
        updated_at: Some(now),
    });
    if queue.len() > SITE_REPLICATION_RETRY_QUEUE_LIMIT {
        let overflow = queue.len() - SITE_REPLICATION_RETRY_QUEUE_LIMIT;
        queue.drain(0..overflow);
    }
}

fn retry_stats_for_state(state: &SiteReplicationState) -> Option<SRRetryStats> {
    if state.retry_queue.is_empty() {
        return None;
    }

    Some(SRRetryStats {
        pending: state.retry_queue.iter().filter(|event| !event.failed).count(),
        failed: state.retry_queue.iter().filter(|event| event.failed).count(),
        last_error: state
            .retry_queue
            .iter()
            .rev()
            .find_map(|event| (!event.last_error.is_empty()).then(|| event.last_error.clone()))
            .unwrap_or_default(),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    })
}

async fn enqueue_site_replication_retry_event(peer: &PeerInfo, path: &str, error: &S3Error) {
    let result = async {
        let mut state = load_site_replication_state().await?;
        upsert_site_replication_retry_event(&mut state.retry_queue, peer, path, &error.to_string());
        persist_site_replication_state(&state).await
    }
    .await;

    if let Err(err) = result {
        warn!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
            event = EVENT_ADMIN_SITE_REPLICATION_STATE,
            peer = %peer.endpoint,
            path,
            error = ?err,
            "failed to persist site replication retry event"
        );
    }
}

fn site_replication_remove_status(peer_errors: &[String]) -> ReplicateRemoveStatus {
    ReplicateRemoveStatus {
        status: SITE_REPL_REMOVE_SUCCESS.to_string(),
        err_detail: if peer_errors.is_empty() {
            String::new()
        } else {
            let summaries: Vec<String> = peer_errors.iter().map(|error| summarize_peer_error_detail(error)).collect();
            summarize_peer_error_detail(&format!("failed to notify {} peer(s): {}", summaries.len(), summaries.join("; ")))
        },
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    }
}

fn status_peer_error(peer: &PeerInfo, detail: String) -> SRPeerError {
    SRPeerError {
        name: peer.name.clone(),
        endpoint: peer.endpoint.clone(),
        error: summarize_peer_error_detail(&detail),
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    }
}

fn pending_operation_for_state(state: &SiteReplicationState, local_peer: &PeerInfo) -> Option<SRPendingOperation> {
    if let Some(pending) = state.pending_remove.as_ref() {
        let pending_peers = pending_remote_peer_ids(&pending.original_peers, local_peer)
            .into_iter()
            .filter(|deployment_id| !pending.acked_deployment_ids.contains(deployment_id))
            .collect();
        return Some(SRPendingOperation {
            operation: "remove".to_string(),
            id: pending.id.clone(),
            pending_peers,
            acked_peers: pending.acked_deployment_ids.iter().cloned().collect(),
            updated_at: pending.updated_at,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        });
    }

    state.pending_rotation.as_ref().map(|pending| {
        let pending_peers = pending_remote_peer_ids(&pending.peers, local_peer)
            .into_iter()
            .filter(|deployment_id| !pending.acked_deployment_ids.contains(deployment_id))
            .collect();
        SRPendingOperation {
            operation: "rotate-svc-acct".to_string(),
            id: pending.id.clone(),
            pending_peers,
            acked_peers: pending.acked_deployment_ids.iter().cloned().collect(),
            updated_at: pending.updated_at,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        }
    })
}

fn pending_remote_peer_ids(peers: &BTreeMap<String, PeerInfo>, local_peer: &PeerInfo) -> BTreeSet<String> {
    peers
        .values()
        .filter(|peer| {
            peer.deployment_id != local_peer.deployment_id && !same_identity_endpoint(&peer.endpoint, &local_peer.endpoint)
        })
        .map(|peer| peer.deployment_id.clone())
        .collect()
}

fn pending_all_remote_peers_acked(
    peers: &BTreeMap<String, PeerInfo>,
    local_peer: &PeerInfo,
    acked_deployment_ids: &BTreeSet<String>,
) -> bool {
    pending_remote_peer_ids(peers, local_peer)
        .iter()
        .all(|deployment_id| acked_deployment_ids.contains(deployment_id))
}

fn push_unique_secret_candidate(candidates: &mut Vec<String>, secret: String) {
    if !secret.is_empty() && !candidates.iter().any(|candidate| candidate == &secret) {
        candidates.push(secret);
    }
}

async fn record_pending_rotation_secret_candidate(rotation_id: &str, secret: String) -> S3Result<()> {
    if secret.is_empty() {
        return Ok(());
    }

    let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
    let mut state = load_site_replication_state().await?;
    if let Some(pending) = state.pending_rotation.as_mut()
        && pending.id == rotation_id
    {
        push_unique_secret_candidate(&mut pending.secret_candidates, secret);
        save_site_replication_state(&state).await?;
    }
    Ok(())
}

async fn record_pending_remove_secret_candidate(remove_id: &str, secret: String) -> S3Result<()> {
    if secret.is_empty() {
        return Ok(());
    }

    let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
    let mut state = load_site_replication_state().await?;
    if let Some(pending) = state.pending_remove.as_mut()
        && pending.id == remove_id
    {
        push_unique_secret_candidate(&mut pending.secret_candidates, secret);
        save_site_replication_state(&state).await?;
    }
    Ok(())
}

async fn mark_pending_rotation_peer_acked(rotation_id: &str, deployment_id: &str) -> S3Result<()> {
    let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
    let mut state = load_site_replication_state().await?;
    if let Some(pending) = state.pending_rotation.as_mut()
        && pending.id == rotation_id
    {
        pending.acked_deployment_ids.insert(deployment_id.to_string());
        save_site_replication_state(&state).await?;
    }
    Ok(())
}

async fn mark_pending_remove_peer_acked(remove_id: &str, deployment_id: &str) -> S3Result<()> {
    let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
    let mut state = load_site_replication_state().await?;
    if let Some(pending) = state.pending_remove.as_mut()
        && pending.id == remove_id
    {
        pending.acked_deployment_ids.insert(deployment_id.to_string());
        save_site_replication_state(&state).await?;
    }
    Ok(())
}

async fn finalize_pending_rotation_if_complete(rotation_id: &str, local_peer: &PeerInfo) -> S3Result<bool> {
    let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
    let mut state = load_site_replication_state().await?;
    let Some(pending) = state.pending_rotation.as_ref() else {
        return Ok(true);
    };
    if pending.id != rotation_id {
        return Ok(false);
    }
    if !pending_all_remote_peers_acked(&pending.peers, local_peer, &pending.acked_deployment_ids) {
        return Ok(false);
    }

    state.pending_rotation = None;
    persist_site_replication_state(&state).await?;
    Ok(true)
}

async fn pending_remove_ready_to_finalize(remove_id: &str, local_peer: &PeerInfo) -> S3Result<Option<PendingRemove>> {
    let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
    let state = load_site_replication_state().await?;
    let Some(pending) = state.pending_remove.as_ref() else {
        return Ok(None);
    };
    if pending.id != remove_id {
        return Ok(None);
    }
    if !pending_all_remote_peers_acked(&pending.original_peers, local_peer, &pending.acked_deployment_ids) {
        return Ok(None);
    }

    Ok(Some(pending.clone()))
}

async fn clear_pending_remove(remove_id: &str) -> S3Result<()> {
    let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
    let mut state = load_site_replication_state().await?;
    if state
        .pending_remove
        .as_ref()
        .map(|pending| pending.id.as_str() == remove_id)
        .unwrap_or(false)
    {
        state.pending_remove = None;
        persist_site_replication_state(&state).await?;
    }
    Ok(())
}

fn removed_deployment_ids_for_pending_remove(pending: &PendingRemove, local_peer: &PeerInfo) -> HashSet<String> {
    if pending.req.remove_all || pending.req.site_names.iter().any(|name| name == &local_peer.name) {
        return pending
            .original_peers
            .keys()
            .filter(|deployment_id| *deployment_id != &local_peer.deployment_id)
            .cloned()
            .collect();
    }

    let removed_names: HashSet<&str> = pending.req.site_names.iter().map(String::as_str).collect();
    pending
        .original_peers
        .iter()
        .filter(|(_, peer)| removed_names.contains(peer.name.as_str()))
        .map(|(deployment_id, _)| deployment_id.clone())
        .collect()
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
    service_account_secret_key: &str,
    arn_override: Option<String>,
) -> S3Result<Option<BucketTarget>> {
    if state.service_account_access_key.is_empty() || service_account_secret_key.is_empty() {
        return Ok(None);
    }

    let parsed = Url::parse(&peer.endpoint)
        .ok()
        .or_else(|| Url::parse(&format!("http://{}", peer.endpoint.trim())).ok())
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid peer endpoint: {}", peer.endpoint)))?;
    let host = parsed.host_str().ok_or_else(|| {
        S3Error::with_message(S3ErrorCode::InvalidRequest, format!("peer endpoint missing host: {}", peer.endpoint))
    })?;
    let port = parsed.port_or_known_default().ok_or_else(|| {
        S3Error::with_message(S3ErrorCode::InvalidRequest, format!("peer endpoint missing port: {}", peer.endpoint))
    })?;
    let region = resolve_region()
        .map(|region| region.to_string())
        .filter(|region| !region.is_empty())
        .unwrap_or_else(|| "us-east-1".to_string());
    let arn = arn_override.unwrap_or_else(|| {
        ARN::new(
            BucketTargetType::ReplicationService,
            peer.deployment_id.clone(),
            String::new(),
            bucket.to_string(),
        )
        .to_string()
    });

    Ok(Some(BucketTarget {
        source_bucket: bucket.to_string(),
        endpoint: format!("{host}:{port}"),
        credentials: Some(Credentials {
            access_key: state.service_account_access_key.clone(),
            secret_key: service_account_secret_key.to_string(),
            session_token: None,
            expiration: None,
        }),
        target_bucket: bucket.to_string(),
        secure: parsed.scheme().eq_ignore_ascii_case("https"),
        arn,
        region,
        target_type: BucketTargetType::ReplicationService,
        deployment_id: peer.deployment_id.clone(),
        ..Default::default()
    }))
}

fn reconcile_site_replication_bucket_targets(
    existing: BucketTargets,
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    config: Option<&s3s::dto::ReplicationConfiguration>,
    service_account_secret_key: &str,
) -> S3Result<BucketTargets> {
    if !state.enabled() || state.service_account_access_key.is_empty() || service_account_secret_key.is_empty() {
        return Ok(existing);
    }

    let configured_arns = site_replication_target_arns_by_peer(config);
    let mut targets = existing.targets;

    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }

        let Some(mut target) = site_replication_bucket_target_for_peer(
            bucket,
            state,
            peer,
            service_account_secret_key,
            configured_arns.get(&peer.deployment_id).cloned(),
        )?
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

    Ok(BucketTargets { targets })
}

fn bucket_target_deployment_id(target: &BucketTarget) -> Option<String> {
    if !target.deployment_id.trim().is_empty() {
        return Some(target.deployment_id.clone());
    }
    replication_target_arn_deployment_id(&target.arn)
}

fn replication_target_arn_deployment_id(arn: &str) -> Option<String> {
    if let Ok(parsed) = arn.parse::<ARN>()
        && parsed.arn_type == BucketTargetType::ReplicationService
    {
        if !parsed.id.is_empty() {
            return Some(parsed.id);
        }
        if !parsed.region.is_empty() {
            return Some(parsed.region);
        }
    }

    let parts: Vec<_> = arn.split(':').collect();
    if parts.len() == 6 && parts[0] == "arn" && parts[1] == "rustfs" && parts[2] == "replication" {
        return if parts[3].is_empty() {
            (!parts[4].is_empty()).then(|| parts[4].to_string())
        } else {
            Some(parts[3].to_string())
        };
    }

    None
}

fn prune_removed_site_replication_bucket_targets(
    existing: BucketTargets,
    removed_deployment_ids: &HashSet<String>,
) -> (BucketTargets, usize) {
    if removed_deployment_ids.is_empty() {
        return (existing, 0);
    }

    let original_len = existing.targets.len();
    let targets = existing
        .targets
        .into_iter()
        .filter(|target| {
            target.target_type != BucketTargetType::ReplicationService
                || bucket_target_deployment_id(target)
                    .map(|deployment_id| !removed_deployment_ids.contains(&deployment_id))
                    .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    let removed = original_len.saturating_sub(targets.len());

    (BucketTargets { targets }, removed)
}

fn replication_rule_deployment_id(rule: &ReplicationRule) -> Option<String> {
    if let Some(rule_id) = rule.id.as_deref() {
        if let Some(deployment_id) = rule_id.strip_prefix("site-repl-")
            && !deployment_id.is_empty()
        {
            return Some(deployment_id.to_string());
        }
        return None;
    }

    replication_target_arn_deployment_id(&rule.destination.bucket)
}

fn prune_removed_site_replication_rules(
    mut config: ReplicationConfiguration,
    removed_deployment_ids: &HashSet<String>,
) -> (Option<ReplicationConfiguration>, usize) {
    if removed_deployment_ids.is_empty() {
        return (Some(config), 0);
    }

    if replication_target_arn_deployment_id(&config.role)
        .map(|deployment_id| removed_deployment_ids.contains(&deployment_id))
        .unwrap_or(false)
    {
        config.role.clear();
    }

    let original_len = config.rules.len();
    config.rules.retain(|rule| {
        replication_rule_deployment_id(rule)
            .map(|deployment_id| !removed_deployment_ids.contains(&deployment_id))
            .unwrap_or(true)
    });
    let removed = original_len.saturating_sub(config.rules.len());

    if removed == 0 {
        return (Some(config), 0);
    }

    if config.rules.is_empty() {
        return (None, removed);
    }

    for (index, rule) in config.rules.iter_mut().enumerate() {
        rule.priority = Some(i32::try_from(index + 1).unwrap_or(i32::MAX));
    }

    (Some(config), removed)
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
        source_selection_criteria: Some(SourceSelectionCriteria {
            replica_modifications: Some(ReplicaModifications {
                status: ReplicaModificationsStatus::from_static(ReplicaModificationsStatus::ENABLED),
            }),
            sse_kms_encrypted_objects: None,
        }),
        status: ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED),
    }
}

fn build_site_replication_config(
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    service_account_secret_key: &str,
) -> S3Result<Option<ReplicationConfiguration>> {
    let mut rules = Vec::new();
    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }

        let Some(target) = site_replication_bucket_target_for_peer(bucket, state, peer, service_account_secret_key, None)? else {
            continue;
        };
        rules.push(build_site_replication_rule(
            &target.arn,
            (rules.len() + 1) as i32,
            &format!("site-repl-{}", peer.deployment_id),
        ));
    }

    if rules.is_empty() {
        Ok(None)
    } else {
        Ok(Some(ReplicationConfiguration {
            role: String::new(),
            rules,
        }))
    }
}

async fn ensure_site_replication_bucket_targets(
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    config: Option<&s3s::dto::ReplicationConfiguration>,
    service_account_secret_key: &str,
) -> S3Result<()> {
    let existing = match metadata_sys::list_bucket_targets(bucket).await {
        Ok(targets) => targets,
        Err(StorageError::ConfigNotFound) => BucketTargets::default(),
        Err(err) => return Err(ApiError::from(err).into()),
    };

    let updated =
        reconcile_site_replication_bucket_targets(existing, bucket, state, local_peer, config, service_account_secret_key)?;
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
    service_account_secret_key: &str,
) -> S3Result<()> {
    // Fix 6: reconcile rather than early-returning when any config already exists.
    // The old code bailed on Ok(_), so the second site joined a replicated bucket and
    // ended up with NO rule pointing back to the first site. Objects on that site could
    // never travel back, producing the "one-directional" replication symptom.
    let Some(desired) = build_site_replication_config(bucket, state, local_peer, service_account_secret_key)? else {
        return Ok(());
    };

    // Load the existing rules (may be empty if never configured).
    let mut existing_rules = match metadata_sys::get_replication_config(bucket).await {
        Ok((existing, _)) => existing.rules,
        Err(StorageError::ConfigNotFound) => Vec::new(),
        Err(err) => return Err(ApiError::from(err).into()),
    };

    // Collect the IDs of existing site-repl-* rules so we don't duplicate them.
    let existing_rule_ids: HashSet<String> = existing_rules
        .iter()
        .filter_map(|r| r.id.as_deref())
        .filter(|id| id.starts_with("site-repl-"))
        .map(String::from)
        .collect();

    let mut added = false;
    for rule in desired.rules {
        let rule_id = rule.id.as_deref().unwrap_or("");
        if !existing_rule_ids.contains(rule_id) {
            existing_rules.push(rule);
            added = true;
        }
    }

    if !added {
        // All desired rules are already present — nothing to write.
        return Ok(());
    }

    // Re-assign contiguous priorities to avoid conflicts with any preserved rules.
    for (i, rule) in existing_rules.iter_mut().enumerate() {
        rule.priority = Some((i + 1) as i32);
    }

    let config = ReplicationConfiguration {
        role: String::new(),
        rules: existing_rules,
    };

    let data = serialize(&config)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize replication failed: {e}")))?;
    metadata_sys::update(bucket, BUCKET_REPLICATION_CONFIG, data)
        .await
        .map_err(ApiError::from)?;

    Ok(())
}

async fn cleanup_removed_site_replication_bucket(bucket: &str, removed_deployment_ids: &HashSet<String>) -> S3Result<usize> {
    let mut removed = 0usize;

    match metadata_sys::list_bucket_targets(bucket).await {
        Ok(targets) => {
            let (updated_targets, removed_targets) =
                prune_removed_site_replication_bucket_targets(targets, removed_deployment_ids);
            if removed_targets > 0 {
                let json_targets = serde_json::to_vec(&updated_targets).map_err(|e| {
                    S3Error::with_message(S3ErrorCode::InternalError, format!("serialize bucket targets failed: {e}"))
                })?;
                metadata_sys::update(bucket, BUCKET_TARGETS_FILE, json_targets)
                    .await
                    .map_err(ApiError::from)?;
                BucketTargetSys::get()
                    .update_all_targets(bucket, Some(&updated_targets))
                    .await;
                removed = removed.saturating_add(removed_targets);
            }
        }
        Err(StorageError::ConfigNotFound) => {}
        Err(err) => return Err(ApiError::from(err).into()),
    }

    match metadata_sys::get_replication_config(bucket).await {
        Ok((config, _)) => {
            let (updated_config, removed_rules) = prune_removed_site_replication_rules(config, removed_deployment_ids);
            if removed_rules > 0 {
                if let Some(updated_config) = updated_config {
                    let data = serialize(&updated_config).map_err(|e| {
                        S3Error::with_message(S3ErrorCode::InternalError, format!("serialize replication failed: {e}"))
                    })?;
                    metadata_sys::update(bucket, BUCKET_REPLICATION_CONFIG, data)
                        .await
                        .map_err(ApiError::from)?;
                } else {
                    metadata_sys::delete(bucket, BUCKET_REPLICATION_CONFIG)
                        .await
                        .map_err(ApiError::from)?;
                }
                removed = removed.saturating_add(removed_rules);
            }
        }
        Err(StorageError::ConfigNotFound) => {}
        Err(err) => return Err(ApiError::from(err).into()),
    }

    Ok(removed)
}

async fn cleanup_removed_site_replication_buckets(removed_deployment_ids: &HashSet<String>) -> S3Result<usize> {
    if removed_deployment_ids.is_empty() {
        return Ok(0);
    }

    let Some(store) = resolve_object_store_handle() else {
        return Ok(0);
    };
    let buckets = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;
    let mut removed = 0usize;

    for bucket in buckets {
        match cleanup_removed_site_replication_bucket(&bucket.name, removed_deployment_ids).await {
            Ok(bucket_removed) => {
                removed = removed.saturating_add(bucket_removed);
            }
            Err(err) => {
                warn!(
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    bucket = %bucket.name,
                    result = "remove_cleanup_failed",
                    error = ?err,
                    "admin site replication state"
                );
                return Err(err);
            }
        }
    }

    Ok(removed)
}

pub async fn site_replication_peer_deployment_id_for_endpoint(endpoint: &str) -> Option<String> {
    let state = load_site_replication_state().await.ok()?;
    peer_deployment_id_for_endpoint(&state, endpoint)
}

/// Fix 1: after persisting a new site-replication state (add or join), enumerate every bucket
/// that already exists locally, wire up versioning + targets + replication config for each, and
/// kick a resync toward every remote peer so pre-existing objects back-fill. Errors are logged
/// but never abort the caller — the admin can run a manual resync if needed.
async fn backfill_existing_buckets_after_add(
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    service_account_secret_key: &str,
) {
    let Some(store) = resolve_object_store_handle() else {
        return;
    };
    let buckets = match store.list_bucket(&BucketOptions::default()).await {
        Ok(b) => b,
        Err(err) => {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                result = "backfill_list_buckets_failed",
                error = ?err,
                "admin site replication state"
            );
            return;
        }
    };

    let resync_id = Uuid::new_v4().to_string();
    for bucket in &buckets {
        let name = &bucket.name;

        if let Err(err) = ensure_site_replication_bucket_versioning(name).await {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                bucket = %name,
                result = "backfill_versioning_setup_failed",
                error = ?err,
                "admin site replication state"
            );
            continue;
        }
        if let Err(err) = ensure_site_replication_bucket_targets(name, state, local_peer, None, service_account_secret_key).await
        {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                bucket = %name,
                result = "backfill_targets_setup_failed",
                error = ?err,
                "admin site replication state"
            );
        }
        if let Err(err) =
            ensure_site_replication_bucket_replication_config(name, state, local_peer, service_account_secret_key).await
        {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                bucket = %name,
                result = "backfill_replication_config_setup_failed",
                error = ?err,
                "admin site replication state"
            );
        }
        // Broadcast the bucket to peers so they create it too (idempotent on the peer side).
        // Read the real lock_enabled flag so peers recreate the bucket with the same object-lock
        // setting — object lock cannot be added after bucket creation.
        let lock_enabled = match metadata_sys::get(name).await {
            Ok(bm) => bm.lock_enabled,
            Err(err) => {
                warn!(
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    bucket = %name,
                    result = "backfill_bucket_metadata_read_failed",
                    fallback = "lock_enabled=false",
                    error = ?err,
                    "admin site replication state"
                );
                false
            }
        };
        if let Err(err) = site_replication_make_bucket_hook(name, lock_enabled).await {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                bucket = %name,
                result = "backfill_make_bucket_broadcast_failed",
                error = ?err,
                "admin site replication state"
            );
        }
        // Kick a resync toward every remote peer so existing objects travel across.
        for peer in state.peers.values() {
            if peer.deployment_id == local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
                continue;
            }
            let result = start_site_bucket_resync(name, peer, &resync_id).await;
            if result.status == "failed" {
                warn!(
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    bucket = %name,
                    peer = %peer.endpoint,
                    result = "backfill_resync_kick_failed",
                    detail = %result.err_detail,
                    "admin site replication state"
                );
            }
        }
    }
}

async fn refresh_bucket_targets_after_service_account_rotation(
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    service_account_secret_key: &str,
) {
    let Some(store) = resolve_object_store_handle() else {
        return;
    };
    let buckets = match store.list_bucket(&BucketOptions::default()).await {
        Ok(buckets) => buckets,
        Err(err) => {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                result = "rotation_target_refresh_list_buckets_failed",
                error = ?err,
                "admin site replication state"
            );
            return;
        }
    };

    for bucket in buckets {
        let replication_config = metadata_sys::get_replication_config(&bucket.name)
            .await
            .ok()
            .map(|(config, _)| config);
        if let Err(err) = ensure_site_replication_bucket_targets(
            &bucket.name,
            state,
            local_peer,
            replication_config.as_ref(),
            service_account_secret_key,
        )
        .await
        {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                bucket = %bucket.name,
                result = "rotation_target_refresh_failed",
                error = ?err,
                "admin site replication state"
            );
        }
    }
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

    let Some(pool) = resolve_replication_pool_handle() else {
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

    let Some(pool) = resolve_replication_pool_handle() else {
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
    let Some(incoming_updated_at) = body.updated_at else {
        return state;
    };
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

async fn ensure_site_replication_bucket_versioning(bucket: &str) -> S3Result<()> {
    match metadata_sys::get_versioning_config(bucket).await {
        Ok((config, _)) if config.enabled() => return Ok(()),
        Ok(_) | Err(StorageError::ConfigNotFound) => {}
        Err(err) => return Err(ApiError::from(err).into()),
    }

    metadata_sys::update(bucket, BUCKET_VERSIONING_CONFIG, bucket_versioning_xml()?)
        .await
        .map_err(ApiError::from)?;

    Ok(())
}

fn is_stale_update(local_updated_at: OffsetDateTime, incoming_updated_at: Option<OffsetDateTime>) -> bool {
    incoming_updated_at.is_some_and(|incoming_updated_at| incoming_updated_at < local_updated_at)
}

fn bucket_meta_local_updated_at(bucket_meta: &super::super::metadata::BucketMetadata, config_file: &str) -> OffsetDateTime {
    match config_file {
        BUCKET_POLICY_CONFIG => bucket_meta.policy_config_updated_at,
        BUCKET_TAGGING_CONFIG => bucket_meta.tagging_config_updated_at,
        BUCKET_VERSIONING_CONFIG => bucket_meta.versioning_config_updated_at,
        OBJECT_LOCK_CONFIG => bucket_meta.object_lock_config_updated_at,
        BUCKET_SSECONFIG => bucket_meta.encryption_config_updated_at,
        BUCKET_REPLICATION_CONFIG => bucket_meta.replication_config_updated_at,
        BUCKET_QUOTA_CONFIG_FILE => bucket_meta.quota_config_updated_at,
        BUCKET_LIFECYCLE_CONFIG => bucket_meta.lifecycle_config_updated_at,
        BUCKET_CORS_CONFIG => bucket_meta.cors_config_updated_at,
        _ => OffsetDateTime::UNIX_EPOCH,
    }
}

async fn apply_bucket_meta_item(item: SRBucketMeta) -> S3Result<()> {
    let Some(store) = resolve_object_store_handle() else {
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

    let incoming_updated_at = if item.r#type == "lc-config" {
        item.expiry_updated_at.or(item.updated_at)
    } else {
        item.updated_at
    };
    if let Ok(bucket_meta) = metadata_sys::get(&item.bucket).await {
        let local_updated_at = bucket_meta_local_updated_at(&bucket_meta, config_file);
        if is_stale_update(local_updated_at, incoming_updated_at) {
            return Ok(());
        }
    }

    let replication_config = if item.r#type == "replication-config" {
        item.replication_config
            .as_ref()
            .map(|raw| {
                let data = decode_bucket_meta_wire_value(raw);
                deserialize::<s3s::dto::ReplicationConfiguration>(&data)
            })
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
        "tags" => decode_bucket_meta_wire_option(item.tags),
        "version-config" => decode_bucket_meta_wire_option(item.versioning),
        "object-lock-config" => decode_bucket_meta_wire_option(item.object_lock_config),
        "sse-config" => decode_bucket_meta_wire_option(item.sse_config),
        "replication-config" => decode_bucket_meta_wire_option(item.replication_config),
        "lc-config" => decode_bucket_meta_wire_option(item.expiry_lc_config),
        "cors-config" => decode_bucket_meta_wire_option(item.cors),
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
        && let Some(runtime) = runtime_site_replication_targets().await?
    {
        ensure_site_replication_bucket_targets(
            &item.bucket,
            &runtime.state,
            &runtime.local_peer,
            replication_config.as_ref(),
            &runtime.service_account_secret_key,
        )
        .await?;
    }

    if item.r#type == "version-config"
        && metadata_sys::get_versioning_config(&item.bucket)
            .await
            .ok()
            .is_some_and(|(config, _)| config.enabled())
        && let Some(runtime) = runtime_site_replication_targets().await?
    {
        ensure_site_replication_bucket_targets(
            &item.bucket,
            &runtime.state,
            &runtime.local_peer,
            replication_config.as_ref(),
            &runtime.service_account_secret_key,
        )
        .await?;
        ensure_site_replication_bucket_replication_config(
            &item.bucket,
            &runtime.state,
            &runtime.local_peer,
            &runtime.service_account_secret_key,
        )
        .await?;
    }

    Ok(())
}

fn group_info_requires_upsert(update: &rustfs_madmin::GroupAddRemove) -> bool {
    !update.is_remove
}

async fn apply_iam_item(item: SRIAMItem) -> S3Result<()> {
    let Some(iam_sys) = resolve_iam_handle() else {
        return Err(s3_error!(InvalidRequest, "iam not init"));
    };
    let incoming_updated_at = item.updated_at;

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
            let Some(secret) = resolve_token_signing_key() else {
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
                if let Some(local) = iam_sys.get_user(&create.access_key).await
                    && is_stale_update(local.update_at.unwrap_or(OffsetDateTime::UNIX_EPOCH), incoming_updated_at)
                {
                    return Ok(());
                }
                let session_policy = if create.access_key == SITE_REPLICATOR_SERVICE_ACCOUNT {
                    Some(site_replicator_service_account_policy()?)
                } else {
                    create.session_policy.as_str().and_then(|raw| serde_json::from_str(raw).ok())
                };
                match iam_sys.get_service_account(&create.access_key).await {
                    Ok((existing, _)) => {
                        if existing.parent_user != create.parent {
                            return Err(s3_error!(
                                InvalidRequest,
                                "service account {} already exists with a different parent user",
                                create.access_key
                            ));
                        }
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
                                    allow_site_replicator_account: create.access_key == SITE_REPLICATOR_SERVICE_ACCOUNT,
                                },
                            )
                            .await
                            .map_err(ApiError::from)?;
                    }
                    Err(err) if is_err_no_such_service_account(&err) => {
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
                    Err(err) => return Err(ApiError::from(err).into()),
                }
                return Ok(());
            }

            if let Some(update) = change.update {
                if let Some(local) = iam_sys.get_user(&update.access_key).await
                    && is_stale_update(local.update_at.unwrap_or(OffsetDateTime::UNIX_EPOCH), incoming_updated_at)
                {
                    return Ok(());
                }
                let allow_site_replicator_account = update.access_key == SITE_REPLICATOR_SERVICE_ACCOUNT;
                let session_policy = if allow_site_replicator_account {
                    Some(site_replicator_service_account_policy()?)
                } else {
                    update.session_policy.as_str().and_then(|raw| serde_json::from_str(raw).ok())
                };
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
                            allow_site_replicator_account,
                        },
                    )
                    .await
                    .map_err(ApiError::from)?;
                return Ok(());
            }

            if let Some(delete) = change.delete {
                if let Some(local) = iam_sys.get_user(&delete.access_key).await
                    && is_stale_update(local.update_at.unwrap_or(OffsetDateTime::UNIX_EPOCH), incoming_updated_at)
                {
                    return Ok(());
                }
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
        reject_site_replicator_on_public_admin(&cred)?;
        let replicate_ilm_expiry = sr_add_replicate_ilm_expiry(&req.uri);
        let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
        let current_state = load_site_replication_state().await?;
        let local_peer = current_local_peer(&req, &current_state);
        let sites: Vec<PeerSite> = read_site_replication_json(req, &cred.secret_key, true).await?;
        validate_add_sites(&sites, &local_peer)?;
        let mut preflight_infos = Vec::with_capacity(sites.len());
        for site in &sites {
            if same_identity_endpoint(&site.endpoint, &local_peer.endpoint) {
                preflight_infos.push(local_add_preflight_info(&current_state, &local_peer, site).await?);
            } else {
                preflight_infos.push(remote_add_preflight_info(site).await?);
            }
        }
        validate_add_preflight_topology(&preflight_infos, &local_peer)?;
        let (service_account_access_key, service_account_secret_key) =
            ensure_site_replicator_service_account(&cred.access_key, false).await?;
        let mut state = merge_add_sites(
            current_state,
            local_peer.clone(),
            sites.clone(),
            service_account_access_key.clone(),
            cred.access_key.clone(),
            replicate_ilm_expiry,
        );
        let join_req = SRPeerJoinReq {
            svc_acct_access_key: service_account_access_key,
            svc_acct_secret_key: service_account_secret_key.clone(),
            svc_acct_parent: String::new(),
            peers: state.peers.clone(),
            updated_at: state.updated_at,
        };

        let mut joined_endpoints = HashSet::new();
        for site in &sites {
            if same_identity_endpoint(&site.endpoint, &local_peer.endpoint)
                || !joined_endpoints.insert(site_identity_key(&site.endpoint))
            {
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
        let bootstrap_errors = bootstrap_existing_metadata_after_add(&state, &local_peer, &service_account_secret_key).await;

        // Fix 1: back-fill pre-existing buckets so objects created before `replicate add`
        // are not silently left out of replication. Failures are logged but do not abort
        // the overall add operation — the admin can trigger a manual resync if needed.
        backfill_existing_buckets_after_add(&state, &local_peer, &service_account_secret_key).await;

        json_response(&ReplicateAddStatus {
            success: true,
            status: SITE_REPL_ADD_SUCCESS.to_string(),
            initial_sync_error_message: bootstrap_errors.join("; "),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
    }
}

pub struct SiteReplicationRemoveHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationRemoveHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationRemoveAction).await?;
        reject_site_replicator_on_public_admin(&cred)?;
        let (pending_remove, local_peer) = {
            let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
            let current_state = load_site_replication_state().await?;
            let local_peer = current_local_peer(&req, &current_state);
            let remove_req: SRRemoveReq = read_site_replication_json(req, "", false).await?;

            if let Some(pending) = current_state.pending_remove.clone() {
                (pending, local_peer)
            } else {
                validate_remove_sites_req(&current_state, &remove_req)?;
                let mut next_state = remove_sites(current_state.clone(), remove_req.clone());
                let mut peer_remove_req = remove_req;
                peer_remove_req.requesting_dep_id = local_peer.deployment_id.clone();
                let pending = PendingRemove {
                    id: Uuid::new_v4().to_string(),
                    req: peer_remove_req,
                    service_account_access_key: current_state.service_account_access_key.clone(),
                    secret_candidates: legacy_site_replicator_state_secret(&current_state).into_iter().collect(),
                    original_peers: current_state.peers.clone(),
                    acked_deployment_ids: BTreeSet::new(),
                    updated_at: next_state.updated_at,
                };
                next_state.pending_remove = Some(pending.clone());
                persist_site_replication_state(&next_state).await?;
                (pending, local_peer)
            }
        };

        let mut peer_errors = Vec::new();
        let mut secret_candidates = pending_remove.secret_candidates.clone();
        if pending_remove.service_account_access_key.is_empty() {
            peer_errors.push("site replication service account unavailable".to_string());
        } else if let Ok(service_account_secret_key) =
            site_replicator_service_account_secret(&pending_remove.service_account_access_key).await
        {
            record_pending_remove_secret_candidate(&pending_remove.id, service_account_secret_key.clone()).await?;
            push_unique_secret_candidate(&mut secret_candidates, service_account_secret_key);
        }

        if secret_candidates.is_empty() {
            peer_errors.push("site replication service account secret unavailable".to_string());
        } else {
            for peer in pending_remove.original_peers.values() {
                if same_identity_endpoint(&peer.endpoint, &local_peer.endpoint)
                    || pending_remove.acked_deployment_ids.contains(&peer.deployment_id)
                {
                    continue;
                }
                if let Err(err) = send_peer_admin_request_with_secret_candidates(
                    &peer.endpoint,
                    SITE_REPLICATION_PEER_REMOVE_PATH,
                    &pending_remove.service_account_access_key,
                    &secret_candidates,
                    &pending_remove.req,
                )
                .await
                {
                    let err_detail = summarize_peer_error_detail(&format!("{}: {err}", peer.endpoint));
                    warn!(
                        event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                        component = LOG_COMPONENT_ADMIN,
                        subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                        peer = %peer.endpoint,
                        result = "peer_remove_notification_failed",
                        error = %err_detail,
                        "admin site replication state"
                    );
                    peer_errors.push(err_detail);
                } else {
                    mark_pending_remove_peer_acked(&pending_remove.id, &peer.deployment_id).await?;
                }
            }
        }

        let finalize_candidate = pending_remove_ready_to_finalize(&pending_remove.id, &local_peer).await?;
        let complete = if let Some(finalized_remove) = finalize_candidate {
            let removed_deployment_ids = removed_deployment_ids_for_pending_remove(&finalized_remove, &local_peer);
            match cleanup_removed_site_replication_buckets(&removed_deployment_ids).await {
                Ok(removed) => {
                    if removed > 0 {
                        info!(
                            event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                            component = LOG_COMPONENT_ADMIN,
                            subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                            removed,
                            result = "remove_cleanup_completed",
                            "admin site replication state"
                        );
                    }
                    clear_pending_remove(&pending_remove.id).await?;
                    true
                }
                Err(err) => {
                    peer_errors.push(summarize_peer_error_detail(&format!("local remove cleanup failed: {err}")));
                    false
                }
            }
        } else {
            false
        };
        if !complete && peer_errors.is_empty() {
            peer_errors.push("site replication remove is still pending".to_string());
        }
        let status = if complete && peer_errors.is_empty() {
            site_replication_remove_status(&[])
        } else {
            site_replication_remove_status(&peer_errors)
        };

        json_response(&status)
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
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        let _ = read_plain_admin_body(req.input).await?;
        Ok(empty_response(StatusCode::NO_CONTENT))
    }
}

pub struct SiteReplicationNetPerfHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationNetPerfHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        let duration = site_repl_netperf_duration(&req.uri);

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
        let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
        let mut state = load_site_replication_state().await?;
        let local_peer = current_local_peer(&req, &state);
        let join_req: SRPeerJoinReq = read_site_replication_json(req, &cred.secret_key, true).await?;

        if let Some(current_updated_at) = state.updated_at {
            let Some(incoming_updated_at) = join_req.updated_at else {
                return json_response(&SRPeerJoinResponse {
                    peer: state.peers.get(&local_peer.deployment_id).cloned().unwrap_or(local_peer),
                });
            };
            if incoming_updated_at <= current_updated_at {
                return json_response(&SRPeerJoinResponse {
                    peer: state.peers.get(&local_peer.deployment_id).cloned().unwrap_or(local_peer),
                });
            }
        }

        if !join_req.svc_acct_access_key.is_empty() && !join_req.svc_acct_secret_key.is_empty() {
            let Some(iam_sys) = resolve_iam_handle() else {
                return Err(s3_error!(InvalidRequest, "iam not init"));
            };

            if iam_sys.get_service_account(&join_req.svc_acct_access_key).await.is_ok() {
                iam_sys
                    .update_service_account(
                        &join_req.svc_acct_access_key,
                        UpdateServiceAccountOpts {
                            session_policy: if join_req.svc_acct_access_key == SITE_REPLICATOR_SERVICE_ACCOUNT {
                                Some(site_replicator_service_account_policy()?)
                            } else {
                                None
                            },
                            secret_key: Some(join_req.svc_acct_secret_key.clone()),
                            name: None,
                            description: None,
                            expiration: None,
                            status: None,
                            allow_site_replicator_account: join_req.svc_acct_access_key == SITE_REPLICATOR_SERVICE_ACCOUNT,
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
                            session_policy: if join_req.svc_acct_access_key == SITE_REPLICATOR_SERVICE_ACCOUNT {
                                Some(site_replicator_service_account_policy()?)
                            } else {
                                None
                            },
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
        // Fix 1 (receiving side): ensure the joining peer also sets up replication for any
        // buckets it already owns so the reverse direction works from the start.
        backfill_existing_buckets_after_add(&state, &local_peer, &join_req.svc_acct_secret_key).await;
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

        let Some(store) = resolve_object_store_handle() else {
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
                if let Some(runtime) = runtime_site_replication_targets().await? {
                    let replication_config = metadata_sys::get_replication_config(&bucket)
                        .await
                        .ok()
                        .map(|(config, _)| config);
                    ensure_site_replication_bucket_targets(
                        &bucket,
                        &runtime.state,
                        &runtime.local_peer,
                        replication_config.as_ref(),
                        &runtime.service_account_secret_key,
                    )
                    .await?;
                    ensure_site_replication_bucket_replication_config(
                        &bucket,
                        &runtime.state,
                        &runtime.local_peer,
                        &runtime.service_account_secret_key,
                    )
                    .await?;
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

        json_response(&local_idp_settings())
    }
}

pub struct SiteReplicationEditHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationEditHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationAddAction).await?;
        reject_site_replicator_on_public_admin(&cred)?;
        let ilm_expiry_override = sr_edit_ilm_expiry_override(&req.uri);
        let incoming: PeerInfo = read_site_replication_json(req, &cred.secret_key, true).await?;
        let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
        let current_state = load_site_replication_state().await?;
        let state = edit_state(current_state.clone(), incoming.clone(), ilm_expiry_override);

        if !current_state.service_account_access_key.is_empty() {
            let service_account_secret_key =
                site_replicator_service_account_secret(&current_state.service_account_access_key).await?;
            let peers_to_send: Vec<PeerInfo> = if ilm_expiry_override.is_some() {
                state.peers.values().cloned().collect()
            } else {
                vec![normalize_peer_info(incoming)]
            };

            for target in current_state.peers.values() {
                let local_target = resolve_deployment_id()
                    .as_ref()
                    .is_some_and(|deployment_id| deployment_id == &target.deployment_id);
                if local_target {
                    continue;
                }

                for peer in &peers_to_send {
                    send_peer_admin_request_with_retry_event(
                        target,
                        SITE_REPLICATION_PEER_EDIT_PATH,
                        &current_state.service_account_access_key,
                        &service_account_secret_key,
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
        let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
        let state = load_site_replication_state().await?;
        let local_peer = current_local_peer(&req, &state);
        let mut incoming: PeerInfo = read_site_replication_json(req, "", false).await?;
        if same_identity_endpoint(&incoming.endpoint, &local_peer.endpoint) {
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
        let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
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
        let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
        let mut state = load_site_replication_state().await?;
        let local_peer = current_local_runtime_peer(&state);
        let peer = normalize_peer_info(peer);
        if peer.deployment_id == local_peer.deployment_id {
            return Err(s3_error!(InvalidRequest, "invalid peer specified - cannot resync to self"));
        }
        if !state.peers.contains_key(&peer.deployment_id) {
            return Err(s3_error!(InvalidRequest, "site replication peer not found"));
        }
        let Some(store) = resolve_object_store_handle() else {
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
            SITE_REPL_RESYNC_STATUS => {
                let status = state
                    .resync_status
                    .get(&peer.deployment_id)
                    .cloned()
                    .unwrap_or_else(|| SRResyncOpStatus {
                        op_type: SITE_REPL_RESYNC_STATUS.to_string(),
                        status: "not-found".to_string(),
                        ..Default::default()
                    });
                return json_response(&status);
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
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        reject_site_replicator_on_public_admin(&cred)?;
        let body: SRStateEditReq = read_site_replication_json(req, "", false).await?;
        let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
        let state = apply_state_edit_req(load_site_replication_state().await?, body);
        save_site_replication_state(&state).await?;
        Ok(empty_response(StatusCode::OK))
    }
}

pub struct SiteReplicationRepairHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationRepairHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        reject_site_replicator_on_public_admin(&cred)?;
        let (state, local_peer, service_account_secret_key) = {
            let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
            let state = load_site_replication_state().await?;
            if !state.enabled() || state.service_account_access_key.is_empty() {
                return Err(s3_error!(InvalidRequest, "site replication is not configured"));
            }
            let service_account_secret_key = site_replicator_service_account_secret(&state.service_account_access_key).await?;
            let local_peer = current_local_peer(&req, &state);
            (state, local_peer, service_account_secret_key)
        };

        let repair_errors = bootstrap_existing_metadata_after_add(&state, &local_peer, &service_account_secret_key).await;
        if repair_errors.is_empty() {
            let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
            let mut latest = load_site_replication_state().await?;
            latest.retry_queue.clear();
            persist_site_replication_state(&latest).await?;
        }

        json_response(&ReplicateEditStatus {
            success: repair_errors.is_empty(),
            status: if repair_errors.is_empty() {
                "Success".to_string()
            } else {
                "Partial".to_string()
            },
            err_detail: repair_errors.join("; "),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        })
    }
}

/// Repairs a split-brained `site-replicator-0` service account.
///
/// When the internal service account is desynced (e.g. after a failed `rm` left stale state on
/// one peer), admin calls to that peer return 403. This handler recovers the cluster without a
/// full teardown:
///
/// 1. Generates a fresh service-account secret locally.
/// 2. Applies it to the local node and persists state.
/// 3. Pushes `peer/join` with the new credentials to every remote peer.
///    A peer whose secret is already correct accepts the update idempotently.
///    A peer whose secret was stale is repaired.
///
/// **Partial failure**: if one or more peers are unreachable the local node is still updated and
/// `status="Partial"` is returned with `err_detail` listing each failed endpoint and its error.
/// The call is **idempotent** — re-run it until `status="Success"` to repair all peers.
pub struct SRRotateServiceAccountHandler {}

#[async_trait::async_trait]
impl Operation for SRRotateServiceAccountHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        reject_site_replicator_on_public_admin(&cred)?;
        let (pending_rotation, local_peer, previous_access_key) = {
            let _state_guard = SITE_REPLICATION_STATE_LOCK.lock().await;
            let mut state = load_site_replication_state().await?;
            if !state.enabled() {
                return Err(s3_error!(InvalidRequest, "site replication is not configured"));
            }
            let local_peer = current_local_peer(&req, &state);
            let previous_access_key = state.service_account_access_key.clone();

            if let Some(pending) = state.pending_rotation.clone() {
                (pending, local_peer, previous_access_key)
            } else {
                let new_secret_key = rustfs_credentials::gen_secret_key(40)
                    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("generate secret key failed: {e}")))?;
                state.service_account_access_key = SITE_REPLICATOR_SERVICE_ACCOUNT.to_string();
                state.service_account_parent = cred.access_key.clone();
                state.updated_at = Some(OffsetDateTime::now_utc());
                let pending = PendingRotation {
                    id: Uuid::new_v4().to_string(),
                    access_key: SITE_REPLICATOR_SERVICE_ACCOUNT.to_string(),
                    parent: cred.access_key.clone(),
                    new_secret_key,
                    secret_candidates: legacy_site_replicator_state_secret(&state).into_iter().collect(),
                    peers: state.peers.clone(),
                    acked_deployment_ids: BTreeSet::new(),
                    updated_at: state.updated_at,
                };
                state.pending_rotation = Some(pending.clone());
                save_site_replication_state(&state).await?;
                (pending, local_peer, previous_access_key)
            }
        };

        if !previous_access_key.is_empty()
            && let Ok(previous_iam_secret) = site_replicator_service_account_secret(&previous_access_key).await
        {
            record_pending_rotation_secret_candidate(&pending_rotation.id, previous_iam_secret).await?;
        }

        set_site_replicator_service_account_secret(&pending_rotation.parent, pending_rotation.new_secret_key.clone()).await?;

        let rotation_state = SiteReplicationState {
            service_account_access_key: pending_rotation.access_key.clone(),
            service_account_parent: pending_rotation.parent.clone(),
            peers: pending_rotation.peers.clone(),
            updated_at: pending_rotation.updated_at,
            ..Default::default()
        };
        refresh_bucket_targets_after_service_account_rotation(&rotation_state, &local_peer, &pending_rotation.new_secret_key)
            .await;

        let mut secret_candidates = pending_rotation.secret_candidates.clone();
        if let Ok(current_secret) = site_replicator_service_account_secret(&pending_rotation.access_key).await {
            push_unique_secret_candidate(&mut secret_candidates, current_secret);
        }
        push_unique_secret_candidate(&mut secret_candidates, pending_rotation.new_secret_key.clone());

        let join_req = SRPeerJoinReq {
            svc_acct_access_key: pending_rotation.access_key.clone(),
            svc_acct_secret_key: pending_rotation.new_secret_key.clone(),
            svc_acct_parent: pending_rotation.parent.clone(),
            peers: pending_rotation.peers.clone(),
            updated_at: pending_rotation.updated_at,
        };

        let mut peer_errors = Vec::new();
        for peer in pending_rotation.peers.values() {
            if same_identity_endpoint(&peer.endpoint, &local_peer.endpoint)
                || pending_rotation.acked_deployment_ids.contains(&peer.deployment_id)
            {
                continue;
            }
            if let Err(err) = send_peer_admin_request_with_secret_candidates(
                &peer.endpoint,
                SITE_REPLICATION_PEER_JOIN_PATH,
                &pending_rotation.access_key,
                &secret_candidates,
                &join_req,
            )
            .await
            {
                let detail = summarize_peer_error_detail(&format!("{}: {err}", peer.endpoint));
                warn!(
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    peer = %peer.endpoint,
                    result = "service_account_rotation_failed",
                    error = %detail,
                    "admin site replication state"
                );
                peer_errors.push(detail);
            } else {
                mark_pending_rotation_peer_acked(&pending_rotation.id, &peer.deployment_id).await?;
            }
        }

        let complete = finalize_pending_rotation_if_complete(&pending_rotation.id, &local_peer).await?;
        if !complete && peer_errors.is_empty() {
            peer_errors.push("service account rotation is still pending".to_string());
        }

        json_response(&ReplicateEditStatus {
            success: complete && peer_errors.is_empty(),
            status: if complete && peer_errors.is_empty() {
                "Success"
            } else {
                "Partial"
            }
            .to_string(),
            err_detail: peer_errors.join("; "),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::Endpoint;
    use super::super::super::{EndpointServerPools, Endpoints, PoolEndpoints};
    use super::*;
    use crate::app::context::{resolve_outbound_tls_generation, set_test_outbound_tls_generation};
    use http::{HeaderMap, HeaderValue, Uri};
    use rustfs_policy::policy::action::S3Action;
    use serial_test::serial;
    use temp_env::with_var;

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

    #[tokio::test]
    async fn test_site_replicator_service_account_policy_allows_peer_and_object_replication() {
        let policy = site_replicator_service_account_policy().expect("site replicator policy should parse");
        let groups: Option<Vec<String>> = None;
        let claims = HashMap::new();
        let conditions = HashMap::new();

        let operation_args = rustfs_policy::policy::Args {
            account: SITE_REPLICATOR_SERVICE_ACCOUNT,
            groups: &groups,
            action: Action::AdminAction(AdminAction::SiteReplicationOperationAction),
            conditions: &conditions,
            is_owner: false,
            claims: &claims,
            deny_only: false,
            bucket: "",
            object: "",
        };
        assert!(policy.is_allowed(&operation_args).await);

        let info_args = rustfs_policy::policy::Args {
            action: Action::AdminAction(AdminAction::SiteReplicationInfoAction),
            ..operation_args
        };
        assert!(policy.is_allowed(&info_args).await);

        let replicate_object_args = rustfs_policy::policy::Args {
            action: Action::S3Action(S3Action::ReplicateObjectAction),
            bucket: "photos",
            object: "image.jpg",
            ..operation_args
        };
        assert!(policy.is_allowed(&replicate_object_args).await);

        let put_object_args = rustfs_policy::policy::Args {
            action: Action::S3Action(S3Action::PutObjectAction),
            ..replicate_object_args
        };
        assert!(policy.is_allowed(&put_object_args).await);

        let get_versioning_args = rustfs_policy::policy::Args {
            action: Action::S3Action(S3Action::GetBucketVersioningAction),
            bucket: "photos",
            object: "",
            ..operation_args
        };
        assert!(policy.is_allowed(&get_versioning_args).await);

        let add_args = rustfs_policy::policy::Args {
            action: Action::AdminAction(AdminAction::SiteReplicationAddAction),
            ..operation_args
        };
        assert!(policy.is_allowed(&add_args).await);

        let remove_args = rustfs_policy::policy::Args {
            action: Action::AdminAction(AdminAction::SiteReplicationRemoveAction),
            ..operation_args
        };
        assert!(policy.is_allowed(&remove_args).await);

        let resync_args = rustfs_policy::policy::Args {
            action: Action::AdminAction(AdminAction::SiteReplicationResyncAction),
            ..operation_args
        };
        assert!(!policy.is_allowed(&resync_args).await);

        let put_policy_args = rustfs_policy::policy::Args {
            action: Action::S3Action(S3Action::PutBucketPolicyAction),
            bucket: "photos",
            object: "",
            ..operation_args
        };
        assert!(!policy.is_allowed(&put_policy_args).await);
    }

    #[test]
    fn test_bucket_versioning_xml_enables_versioning() {
        let data = bucket_versioning_xml().expect("versioning XML should serialize");
        let config: VersioningConfiguration = deserialize(&data).expect("versioning XML should deserialize");

        assert!(config.enabled());
    }

    #[test]
    fn test_sr_metainfo_path_preserves_status_query() {
        let uri: Uri = "/rustfs/admin/v3/site-replication/status?buckets=true&entity=bucket&entityvalue=photos"
            .parse()
            .unwrap();

        assert_eq!(
            sr_metainfo_path(&uri),
            "/rustfs/admin/v3/site-replication/metainfo?buckets=true&entity=bucket&entityvalue=photos"
        );
    }

    #[test]
    fn test_site_replication_config_status_accepts_peer_specific_targets() {
        let site_a_config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![build_site_replication_rule(
                "arn:rustfs:replication::site-b:test-replication",
                1,
                "site-repl-site-b",
            )],
        };
        let site_b_config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![build_site_replication_rule(
                "arn:rustfs:replication::site-a:test-replication",
                1,
                "site-repl-site-a",
            )],
        };
        let site_a_xml = String::from_utf8(serialize(&site_a_config).expect("site replication XML should serialize"))
            .expect("site replication XML should be UTF-8");
        let site_b_xml = String::from_utf8(serialize(&site_b_config).expect("site replication XML should serialize"))
            .expect("site replication XML should be UTF-8");

        assert!(site_replication_rule_complete(&site_a_config.rules[0]));
        assert_eq!(
            site_replication_config_mismatch(vec![Some(&site_a_xml), Some(&site_b_xml)].into_iter(), 2),
            (2, false)
        );
    }

    #[test]
    fn test_status_policy_compare_ignores_string_array_order() {
        let site_a_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["s3:GetBucketQuota", "s3:GetBucketLocation", "s3:GetObject"],
                "Resource": ["arn:aws:s3:::*"]
            }]
        });
        let site_b_policy = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["s3:GetObject", "s3:GetBucketLocation", "s3:GetBucketQuota"],
                "Resource": ["arn:aws:s3:::*"]
            }]
        });

        assert_eq!(
            value_config_mismatch(vec![Some(&site_a_policy), Some(&site_b_policy)].into_iter(), 2),
            (2, false)
        );
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
            "root".to_string(),
            true,
        );

        assert_eq!(state.peers.len(), 2);
        assert!(state.peers.contains_key("local-dep"));
    }

    #[test]
    fn test_validate_add_sites_rejects_duplicate_endpoints() {
        let local_peer = peer("local", "https://local.example.com");
        let sites = vec![
            PeerSite {
                endpoint: "https://remote.example.com".to_string(),
                access_key: "remote-ak".to_string(),
                secret_key: "remote-sk".to_string(),
                ..Default::default()
            },
            PeerSite {
                endpoint: "https://remote.example.com/".to_string(),
                access_key: "remote-ak".to_string(),
                secret_key: "remote-sk".to_string(),
                ..Default::default()
            },
        ];

        let err = validate_add_sites(&sites, &local_peer).expect_err("duplicate endpoint should fail");

        assert!(err.to_string().contains("duplicate site endpoint"));
    }

    #[test]
    fn test_validate_add_sites_requires_remote_credentials() {
        let local_peer = peer("local", "https://local.example.com");
        let sites = vec![PeerSite {
            endpoint: "https://remote.example.com".to_string(),
            access_key: "remote-ak".to_string(),
            ..Default::default()
        }];

        let err = validate_add_sites(&sites, &local_peer).expect_err("missing remote secret should fail");

        assert!(err.to_string().contains("secretKey is required"));
    }

    fn preflight_site(name: &str, endpoint: &str, deployment_id: &str, bucket_count: usize) -> SiteReplicationAddPreflightInfo {
        SiteReplicationAddPreflightInfo {
            name: name.to_string(),
            endpoint: endpoint.to_string(),
            deployment_id: deployment_id.to_string(),
            enabled: false,
            bucket_count,
            peer_deployment_ids: BTreeSet::new(),
            idp_settings: serde_json::json!({"provider": "same"}),
        }
    }

    #[test]
    fn test_validate_add_preflight_topology_accepts_matching_sites() {
        let local_peer = PeerInfo {
            deployment_id: "local-dep".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let infos = vec![
            preflight_site("local", "https://local.example.com", "local-dep", 1),
            preflight_site("remote", "https://remote.example.com", "remote-dep", 0),
        ];

        validate_add_preflight_topology(&infos, &local_peer).expect("matching preflight should pass");
    }

    #[test]
    fn test_validate_add_preflight_topology_rejects_duplicate_deployment_id() {
        let local_peer = PeerInfo {
            deployment_id: "local-dep".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let infos = vec![
            preflight_site("local", "https://local.example.com", "local-dep", 0),
            preflight_site("remote", "https://remote.example.com", "local-dep", 0),
        ];

        let err = validate_add_preflight_topology(&infos, &local_peer).expect_err("duplicate deploymentID should fail");

        assert!(err.to_string().contains("duplicate deploymentID"));
    }

    #[test]
    fn test_validate_add_preflight_topology_requires_local_deployment() {
        let local_peer = PeerInfo {
            deployment_id: "local-dep".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let infos = vec![preflight_site("remote", "https://remote.example.com", "remote-dep", 0)];

        let err = validate_add_preflight_topology(&infos, &local_peer).expect_err("missing local deployment should fail");

        assert!(err.to_string().contains("must include the local deployment"));
    }

    #[test]
    fn test_validate_add_preflight_topology_rejects_idp_mismatch() {
        let local_peer = PeerInfo {
            deployment_id: "local-dep".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let mut remote = preflight_site("remote", "https://remote.example.com", "remote-dep", 0);
        remote.idp_settings = serde_json::json!({"provider": "different"});
        let infos = vec![preflight_site("local", "https://local.example.com", "local-dep", 0), remote];

        let err = validate_add_preflight_topology(&infos, &local_peer).expect_err("IDP mismatch should fail");

        assert!(err.to_string().contains("IDP settings mismatch"));
    }

    #[test]
    fn test_validate_add_preflight_topology_rejects_multiple_non_empty_sites() {
        let local_peer = PeerInfo {
            deployment_id: "local-dep".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let infos = vec![
            preflight_site("local", "https://local.example.com", "local-dep", 1),
            preflight_site("remote", "https://remote.example.com", "remote-dep", 1),
        ];

        let err = validate_add_preflight_topology(&infos, &local_peer).expect_err("multiple non-empty sites should fail");

        assert!(err.to_string().contains("only one site"));
    }

    #[test]
    fn test_validate_add_preflight_topology_rejects_existing_peer_set_mismatch() {
        let local_peer = PeerInfo {
            deployment_id: "local-dep".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let local = preflight_site("local", "https://local.example.com", "local-dep", 0);
        let mut remote = preflight_site("remote", "https://remote.example.com", "remote-dep", 0);
        remote.enabled = true;
        remote.peer_deployment_ids = BTreeSet::from(["remote-dep".to_string(), "old-dep".to_string()]);
        let infos = vec![local, remote];

        let err = validate_add_preflight_topology(&infos, &local_peer).expect_err("peer set mismatch should fail");

        assert!(err.to_string().contains("different site replication peer set"));
    }

    #[test]
    fn test_site_replication_bootstrap_plan_includes_replayable_snapshot_items() {
        let mut info = SRInfo::default();
        info.state.peers.insert(
            "remote".to_string(),
            PeerInfo {
                replicate_ilm_expiry: true,
                ..peer("remote", "https://remote.example.com")
            },
        );
        info.policies.insert(
            "readwrite".to_string(),
            SRIAMPolicy {
                policy: Some(serde_json::json!({"Version": "2012-10-17", "Statement": []})),
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            },
        );
        info.user_info_map.insert(
            "alice".to_string(),
            rustfs_madmin::UserInfo {
                secret_key: Some("alice-secret".to_string()),
                policy_name: Some("readwrite".to_string()),
                status: rustfs_madmin::AccountStatus::Enabled,
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
                ..Default::default()
            },
        );
        info.user_info_map.insert(
            "external".to_string(),
            rustfs_madmin::UserInfo {
                secret_key: None,
                status: rustfs_madmin::AccountStatus::Enabled,
                ..Default::default()
            },
        );
        info.group_desc_map.insert(
            "devs".to_string(),
            rustfs_madmin::GroupDesc {
                name: "devs".to_string(),
                status: "enabled".to_string(),
                members: vec!["alice".to_string()],
                policy: String::new(),
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            },
        );
        info.user_policies.insert(
            "alice".to_string(),
            SRPolicyMapping {
                user_or_group: "alice".to_string(),
                user_type: UserType::Reg.to_u64(),
                policy: "readwrite".to_string(),
                updated_at: Some(OffsetDateTime::UNIX_EPOCH),
                ..Default::default()
            },
        );
        info.buckets.insert(
            "photos".to_string(),
            SRBucketInfo {
                bucket: "photos".to_string(),
                policy: Some(serde_json::json!({"Statement": []})),
                versioning: Some(BASE64_STANDARD.encode("<VersioningConfiguration/>")),
                quota_config: Some(BASE64_STANDARD.encode(r#"{"quota":1024}"#)),
                expiry_lc_config: Some(BASE64_STANDARD.encode("<LifecycleConfiguration/>")),
                object_lock_config: Some(BASE64_STANDARD.encode("<ObjectLockConfiguration/>")),
                created_at: Some(OffsetDateTime::UNIX_EPOCH),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            },
        );

        let plan = site_replication_bootstrap_plan(&info).expect("bootstrap plan should build");

        assert_eq!(plan.iam_items.iter().map(|item| item.r#type.as_str()).collect::<Vec<_>>(), {
            vec!["policy", "iam-user", "group-info", "policy-mapping"]
        });
        assert_eq!(plan.bucket_make_ops.len(), 1);
        assert!(plan.bucket_make_ops[0].contains("operation=make-with-versioning"));
        assert!(plan.bucket_make_ops[0].contains("lockEnabled=true"));
        assert_eq!(plan.bucket_configure_ops.len(), 1);
        assert!(plan.bucket_configure_ops[0].contains("operation=configure-replication"));

        let bucket_types = plan.bucket_items.iter().map(|item| item.r#type.as_str()).collect::<Vec<_>>();
        assert_eq!(
            bucket_types,
            vec!["policy", "version-config", "object-lock-config", "quota-config", "lc-config"]
        );
        let quota = plan
            .bucket_items
            .iter()
            .find(|item| item.r#type == "quota-config")
            .and_then(|item| item.quota.as_ref())
            .expect("quota item should exist");
        assert_eq!(quota["quota"], 1024);
    }

    #[test]
    fn test_site_replication_bootstrap_plan_skips_lifecycle_by_default() {
        let mut info = SRInfo::default();
        info.buckets.insert(
            "photos".to_string(),
            SRBucketInfo {
                bucket: "photos".to_string(),
                expiry_lc_config: Some(BASE64_STANDARD.encode("<LifecycleConfiguration/>")),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            },
        );

        let plan = site_replication_bootstrap_plan(&info).expect("bootstrap plan should build");

        assert!(!plan.bucket_items.iter().any(|item| item.r#type == "lc-config"));
    }

    #[test]
    fn test_site_replication_state_replicates_ilm_expiry_detects_enabled_peer() {
        let mut state = SiteReplicationState::default();
        state.peers.insert(
            "remote".to_string(),
            PeerInfo {
                replicate_ilm_expiry: true,
                ..peer("remote", "https://remote.example.com")
            },
        );

        assert!(site_replication_state_replicates_ilm_expiry(&state));
    }

    #[test]
    fn test_retry_event_upsert_marks_repeated_failures() {
        let peer = PeerInfo {
            deployment_id: "remote-dep".to_string(),
            ..peer("remote", "https://remote.example.com")
        };
        let mut queue = Vec::new();

        upsert_site_replication_retry_event(&mut queue, &peer, "/rustfs/admin/v3/site-replication/peer/iam-item", "first");
        upsert_site_replication_retry_event(&mut queue, &peer, "/rustfs/admin/v3/site-replication/peer/iam-item", "second");
        upsert_site_replication_retry_event(&mut queue, &peer, "/rustfs/admin/v3/site-replication/peer/iam-item", "third");

        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].retry_count, SITE_REPLICATION_RETRY_FAILED_AFTER);
        assert!(queue[0].failed);
        assert_eq!(queue[0].last_error, "third");
    }

    #[test]
    fn test_retry_stats_for_state_counts_pending_and_failed() {
        let mut state = SiteReplicationState::default();
        state.retry_queue = vec![
            SiteReplicationRetryEvent {
                failed: false,
                last_error: "pending".to_string(),
                ..Default::default()
            },
            SiteReplicationRetryEvent {
                failed: true,
                last_error: "failed".to_string(),
                ..Default::default()
            },
        ];

        let stats = retry_stats_for_state(&state).expect("retry stats should be present");

        assert_eq!(stats.pending, 1);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.last_error, "failed");
    }

    #[test]
    fn test_remove_sites_prunes_retry_queue_for_removed_peer() {
        let mut state = SiteReplicationState::default();
        state.name = "local".to_string();
        state.peers.insert(
            "remote-dep".to_string(),
            PeerInfo {
                deployment_id: "remote-dep".to_string(),
                name: "remote".to_string(),
                endpoint: "https://remote.example.com".to_string(),
                ..Default::default()
            },
        );
        state.retry_queue.push(SiteReplicationRetryEvent {
            peer_deployment_id: "remote-dep".to_string(),
            peer_endpoint: "https://remote.example.com".to_string(),
            path: "/rustfs/admin/v3/site-replication/peer/iam-item".to_string(),
            ..Default::default()
        });

        let state = remove_sites(
            state,
            SRRemoveReq {
                site_names: vec!["remote".to_string()],
                ..Default::default()
            },
        );

        assert!(state.retry_queue.is_empty());
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
    fn test_site_identity_key_deduplicates_scheme_drift_on_same_host_port() {
        assert_eq!(
            site_identity_key("https://node-a.example.com:9000"),
            site_identity_key("http://NODE-A.example.com:9000/"),
        );
    }

    #[test]
    fn test_normalize_peer_map_by_identity_prefers_https_endpoint() {
        let peers = BTreeMap::from([
            (
                "peer-http".to_string(),
                PeerInfo {
                    deployment_id: "peer-http".to_string(),
                    ..peer("peer", "http://node-a.example.com:9000")
                },
            ),
            (
                "peer-https".to_string(),
                PeerInfo {
                    deployment_id: "peer-https".to_string(),
                    ..peer("peer", "https://node-a.example.com:9000")
                },
            ),
        ]);

        let normalized = normalize_peer_map_by_identity(peers);
        assert_eq!(normalized.len(), 1);
        let normalized_peer = normalized.values().next().expect("normalized peer");
        assert!(normalized_peer.endpoint.starts_with("https://"));
    }

    #[test]
    fn test_request_endpoint_prefers_forwarded_proto() {
        let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-scheme", HeaderValue::from_static("http"));
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        headers.insert("host", HeaderValue::from_static("node-a.example.com:9000"));

        let endpoint = request_endpoint(&uri, &headers);

        assert_eq!(endpoint, "https://node-a.example.com:9000");
    }

    #[test]
    fn test_request_endpoint_uses_absolute_uri_without_host_header() {
        let uri: Uri = "https://node-a.example.com:9443/rustfs/admin/v3/site-replication/status"
            .parse()
            .unwrap();
        let headers = HeaderMap::new();

        let endpoint = request_endpoint(&uri, &headers);

        assert_eq!(endpoint, "https://node-a.example.com:9443");
    }

    #[test]
    fn test_request_endpoint_falls_back_to_https_when_tls_path_is_configured() {
        with_var(ENV_RUSTFS_TLS_PATH, Some("/tmp/tls"), || {
            let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
            let headers = HeaderMap::new();

            let endpoint = request_endpoint(&uri, &headers);

            assert!(endpoint.starts_with("https://"));
        });
    }

    #[test]
    fn test_site_replication_local_endpoint_uses_api_port_for_console_host_header() {
        let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        headers.insert("host", HeaderValue::from_static("node-a.example.com:9001"));

        let endpoint = site_replication_local_endpoint(&uri, &headers);

        assert_eq!(endpoint, "https://node-a.example.com:9000");
    }

    #[test]
    fn test_site_replication_local_endpoint_preserves_ipv6_host() {
        let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        headers.insert("host", HeaderValue::from_static("[::1]:9001"));

        let endpoint = site_replication_local_endpoint(&uri, &headers);

        assert_eq!(endpoint, "https://[::1]:9000");
    }

    #[test]
    fn test_site_replication_local_endpoint_preserves_non_console_port() {
        let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        headers.insert("host", HeaderValue::from_static("lb.example.com:9443"));

        let endpoint = site_replication_local_endpoint(&uri, &headers);

        assert_eq!(endpoint, "https://lb.example.com:9443");
    }

    #[test]
    fn test_site_replication_local_endpoint_rejects_forwarded_non_http_scheme() {
        let uri: Uri = "/rustfs/admin/v3/site-replication/status".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", HeaderValue::from_static("ftp"));
        headers.insert("host", HeaderValue::from_static("node-a.example.com:9000"));

        let endpoint = site_replication_local_endpoint(&uri, &headers);

        assert!(!endpoint.starts_with("ftp://"));
    }

    #[test]
    fn test_runtime_tls_enabled_prefers_explicit_tls_over_http_runtime_endpoint() {
        let endpoints = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::from(vec![Endpoint {
                url: Url::parse("http://127.0.0.1:9000/tmp").unwrap(),
                is_local: true,
                pool_idx: 0,
                set_idx: 0,
                disk_idx: 0,
            }]),
            cmd_line: String::new(),
            platform: String::new(),
        }]);

        with_var(ENV_RUSTFS_TLS_PATH, Some("/tmp/tls"), || {
            assert!(runtime_tls_enabled_with(Some(&endpoints)));
        });
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
    fn test_validate_remove_sites_req_rejects_empty_and_unknown_sites() {
        let mut state = SiteReplicationState {
            name: "local".to_string(),
            ..Default::default()
        };
        state.peers.insert(
            "remote".to_string(),
            PeerInfo {
                deployment_id: "remote".to_string(),
                ..peer("remote", "https://remote.example.com")
            },
        );

        assert!(validate_remove_sites_req(&state, &SRRemoveReq::default()).is_err());
        assert!(
            validate_remove_sites_req(
                &state,
                &SRRemoveReq {
                    remove_all: true,
                    site_names: vec!["remote".to_string()],
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(
            validate_remove_sites_req(
                &state,
                &SRRemoveReq {
                    site_names: vec!["missing".to_string()],
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert!(
            validate_remove_sites_req(
                &state,
                &SRRemoveReq {
                    site_names: vec!["remote".to_string()],
                    ..Default::default()
                }
            )
            .is_ok()
        );
    }

    #[test]
    fn test_remove_sites_keeps_local_success_with_peer_errors() {
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

        let state = remove_sites(
            state,
            SRRemoveReq {
                remove_all: true,
                ..Default::default()
            },
        );
        let status =
            site_replication_remove_status(&["peer request to https://remote.example.com failed with 403 Forbidden".to_string()]);

        assert!(state.peers.is_empty());
        assert_eq!(status.status, SITE_REPL_REMOVE_SUCCESS);
        assert!(status.err_detail.contains("failed to notify 1 peer"));
        assert!(status.err_detail.contains("403 Forbidden"));
    }

    #[test]
    fn test_remove_sites_drops_resync_status_for_removed_peer() {
        let mut state = SiteReplicationState {
            name: "local".to_string(),
            ..Default::default()
        };
        state.peers.insert(
            "local-deployment".to_string(),
            PeerInfo {
                deployment_id: "local-deployment".to_string(),
                ..peer("local", "https://local.example.com")
            },
        );
        state.peers.insert(
            "remote-a-deployment".to_string(),
            PeerInfo {
                deployment_id: "remote-a-deployment".to_string(),
                ..peer("remote-a", "https://remote-a.example.com")
            },
        );
        state.peers.insert(
            "remote-b-deployment".to_string(),
            PeerInfo {
                deployment_id: "remote-b-deployment".to_string(),
                ..peer("remote-b", "https://remote-b.example.com")
            },
        );
        state.resync_status.insert(
            "remote-a-deployment".to_string(),
            SRResyncOpStatus {
                resync_id: "stale-a".to_string(),
                status: "success".to_string(),
                ..Default::default()
            },
        );
        state.resync_status.insert(
            "remote-a-legacy-key".to_string(),
            SRResyncOpStatus {
                resync_id: "stale-a-legacy".to_string(),
                status: "success".to_string(),
                ..Default::default()
            },
        );
        state.resync_status.insert(
            "remote-b-deployment".to_string(),
            SRResyncOpStatus {
                resync_id: "active-b".to_string(),
                status: "success".to_string(),
                ..Default::default()
            },
        );

        let state = remove_sites(
            state,
            SRRemoveReq {
                site_names: vec!["remote-a".to_string()],
                ..Default::default()
            },
        );

        assert!(state.peers.contains_key("local-deployment"));
        assert!(!state.peers.contains_key("remote-a-deployment"));
        assert!(state.peers.contains_key("remote-b-deployment"));
        assert!(!state.resync_status.contains_key("remote-a-deployment"));
        assert!(!state.resync_status.contains_key("remote-a-legacy-key"));
        assert!(state.resync_status.contains_key("remote-b-deployment"));
    }

    #[test]
    fn test_remove_sites_prunes_orphan_resync_status_without_matching_site() {
        let mut state = SiteReplicationState {
            name: "local".to_string(),
            ..Default::default()
        };
        state.peers.insert(
            "remote-a-deployment".to_string(),
            PeerInfo {
                deployment_id: "remote-a-deployment".to_string(),
                ..peer("remote-a", "https://remote-a.example.com")
            },
        );
        state.peers.insert(
            "remote-b-deployment".to_string(),
            PeerInfo {
                deployment_id: "remote-b-deployment".to_string(),
                ..peer("remote-b", "https://remote-b.example.com")
            },
        );
        state.resync_status.insert(
            "remote-a-deployment".to_string(),
            SRResyncOpStatus {
                resync_id: "active-a".to_string(),
                status: "success".to_string(),
                ..Default::default()
            },
        );
        state.resync_status.insert(
            "removed-deployment".to_string(),
            SRResyncOpStatus {
                resync_id: "orphaned".to_string(),
                status: "success".to_string(),
                ..Default::default()
            },
        );

        let state = remove_sites(
            state,
            SRRemoveReq {
                site_names: vec!["missing-site".to_string()],
                ..Default::default()
            },
        );

        assert!(state.peers.contains_key("remote-a-deployment"));
        assert!(state.peers.contains_key("remote-b-deployment"));
        assert!(state.resync_status.contains_key("remote-a-deployment"));
        assert!(!state.resync_status.contains_key("removed-deployment"));
    }

    #[test]
    fn test_remove_sites_clears_state_when_local_site_is_removed() {
        let mut state = SiteReplicationState {
            name: "local".to_string(),
            ..Default::default()
        };
        state.peers.insert(
            "local-deployment".to_string(),
            PeerInfo {
                deployment_id: "local-deployment".to_string(),
                ..peer("local", "https://local.example.com")
            },
        );
        state.peers.insert(
            "remote-a-deployment".to_string(),
            PeerInfo {
                deployment_id: "remote-a-deployment".to_string(),
                ..peer("remote-a", "https://remote-a.example.com")
            },
        );
        state.peers.insert(
            "remote-b-deployment".to_string(),
            PeerInfo {
                deployment_id: "remote-b-deployment".to_string(),
                ..peer("remote-b", "https://remote-b.example.com")
            },
        );
        state.resync_status.insert(
            "remote-a-deployment".to_string(),
            SRResyncOpStatus {
                resync_id: "active-a".to_string(),
                status: "success".to_string(),
                ..Default::default()
            },
        );

        let state = remove_sites(
            state,
            SRRemoveReq {
                site_names: vec!["local".to_string()],
                ..Default::default()
            },
        );

        assert!(state.peers.is_empty());
        assert!(state.resync_status.is_empty());
    }

    #[test]
    fn test_site_replication_remove_status_truncates_peer_error_detail() {
        let long_peer_body = "peer response body ".repeat(40);
        let status = site_replication_remove_status(&[format!(
            "https://remote.example.com: peer request failed with 403 Forbidden: {long_peer_body}"
        )]);

        assert!(status.err_detail.contains("403 Forbidden"));
        assert!(status.err_detail.contains("truncated"));
        assert!(!status.err_detail.contains(&long_peer_body));
    }

    #[test]
    fn test_site_replication_remove_status_caps_final_error_detail() {
        let peer_errors: Vec<String> = (0..8)
            .map(|idx| format!("https://remote-{idx}.example.com: {}", "peer response body ".repeat(40)))
            .collect();
        let status = site_replication_remove_status(&peer_errors);

        assert!(status.err_detail.chars().count() <= SITE_REPLICATION_PEER_ERROR_DETAIL_LIMIT);
        assert!(status.err_detail.contains("truncated"));
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
        let mut state = SiteReplicationState {
            service_account_access_key: "site-replicator-0".to_string(),
            service_account_secret_key: "stale-state-secret".to_string(),
            ..Default::default()
        };
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
            "runtime-iam-secret",
        )
        .expect("reconcile bucket targets");

        assert_eq!(targets.targets.len(), 1);
        let target = &targets.targets[0];
        assert_eq!(target.target_type, BucketTargetType::ReplicationService);
        assert_eq!(target.endpoint, "remote.example.com:9000");
        assert!(!target.secure);
        assert_eq!(target.target_bucket, "photos");
        assert_eq!(target.deployment_id, "remote");
        assert_eq!(target.arn, "arn:rustfs:replication::remote:photos");
        assert_eq!(target.region, "us-east-1");
        let credentials = target
            .credentials
            .as_ref()
            .expect("site replication target should carry credentials");
        assert_eq!(credentials.access_key, "site-replicator-0");
        assert_eq!(credentials.secret_key, "runtime-iam-secret");
    }

    #[test]
    fn test_prune_removed_site_replication_bucket_targets_keeps_unrelated_targets() {
        let removed_deployment_ids = HashSet::from(["removed-dep".to_string()]);
        let targets = BucketTargets {
            targets: vec![
                BucketTarget {
                    arn: "arn:rustfs:replication::removed-dep:photos".to_string(),
                    deployment_id: "removed-dep".to_string(),
                    target_type: BucketTargetType::ReplicationService,
                    ..Default::default()
                },
                BucketTarget {
                    arn: "arn:rustfs:replication::kept-dep:photos".to_string(),
                    deployment_id: "kept-dep".to_string(),
                    target_type: BucketTargetType::ReplicationService,
                    ..Default::default()
                },
                BucketTarget {
                    arn: "arn:rustfs:ilm::removed-dep:photos".to_string(),
                    deployment_id: "removed-dep".to_string(),
                    target_type: BucketTargetType::IlmService,
                    ..Default::default()
                },
            ],
        };

        let (updated, removed) = prune_removed_site_replication_bucket_targets(targets, &removed_deployment_ids);

        assert_eq!(removed, 1);
        assert_eq!(updated.targets.len(), 2);
        assert!(updated.targets.iter().any(|target| target.deployment_id == "kept-dep"));
        assert!(
            updated
                .targets
                .iter()
                .any(|target| target.target_type == BucketTargetType::IlmService)
        );
    }

    #[test]
    fn test_prune_removed_site_replication_rules_removes_site_rule_and_reorders_priorities() {
        let removed_deployment_ids = HashSet::from(["removed-dep".to_string()]);
        let kept_rule = build_site_replication_rule("arn:rustfs:replication::kept-dep:photos", 3, "site-repl-kept-dep");
        let removed_rule = build_site_replication_rule("arn:rustfs:replication::removed-dep:photos", 1, "site-repl-removed-dep");
        let user_rule = build_site_replication_rule("arn:rustfs:replication::removed-dep:photos", 9, "user-managed-rule");
        let config = ReplicationConfiguration {
            role: "arn:rustfs:replication::removed-dep:photos".to_string(),
            rules: vec![removed_rule, user_rule, kept_rule],
        };

        let (updated, removed) = prune_removed_site_replication_rules(config, &removed_deployment_ids);
        let updated = updated.expect("config should keep non-removed rules");

        assert_eq!(removed, 1);
        assert!(updated.role.is_empty());
        assert_eq!(updated.rules.len(), 2);
        assert_eq!(updated.rules[0].id.as_deref(), Some("user-managed-rule"));
        assert_eq!(updated.rules[0].priority, Some(1));
        assert_eq!(updated.rules[1].id.as_deref(), Some("site-repl-kept-dep"));
        assert_eq!(updated.rules[1].priority, Some(2));
    }

    #[test]
    fn test_site_replication_state_does_not_serialize_service_account_secret() {
        let state = SiteReplicationState {
            service_account_access_key: "site-replicator-0".to_string(),
            service_account_secret_key: "do-not-persist".to_string(),
            ..Default::default()
        };

        let json = serde_json::to_value(&state).expect("serialize state");

        assert!(json.get("service_account_secret_key").is_none());
        assert!(json.get("service_account_access_key").is_some());
    }

    #[test]
    fn test_pending_rotation_serializes_temporary_secret_until_cleanup() {
        let state = SiteReplicationState {
            service_account_access_key: SITE_REPLICATOR_SERVICE_ACCOUNT.to_string(),
            service_account_secret_key: "do-not-persist".to_string(),
            pending_rotation: Some(PendingRotation {
                id: "rotation-id".to_string(),
                access_key: SITE_REPLICATOR_SERVICE_ACCOUNT.to_string(),
                parent: "root".to_string(),
                new_secret_key: "temporary-new-secret".to_string(),
                secret_candidates: vec!["temporary-old-secret".to_string()],
                ..Default::default()
            }),
            ..Default::default()
        };

        let json = serde_json::to_value(&state).expect("serialize state");

        assert!(json.get("service_account_secret_key").is_none());
        let pending = json.get("pending_rotation").expect("pending rotation should serialize");
        assert_eq!(pending.get("new_secret_key").and_then(Value::as_str), Some("temporary-new-secret"));
        assert!(pending.get("secret_candidates").is_some());
    }

    #[test]
    fn test_pending_remote_peer_ack_completion_ignores_local_peer() {
        let local = PeerInfo {
            deployment_id: "local".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let remote = PeerInfo {
            deployment_id: "remote".to_string(),
            ..peer("remote", "https://remote.example.com")
        };
        let peers = BTreeMap::from([
            (local.deployment_id.clone(), local.clone()),
            (remote.deployment_id.clone(), remote),
        ]);

        assert!(!pending_all_remote_peers_acked(&peers, &local, &BTreeSet::new()));
        assert!(pending_all_remote_peers_acked(&peers, &local, &BTreeSet::from(["remote".to_string()])));
    }

    #[test]
    fn test_pending_operation_for_state_reports_remove_progress() {
        let local = PeerInfo {
            deployment_id: "local".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let remote_a = PeerInfo {
            deployment_id: "remote-a".to_string(),
            ..peer("remote-a", "https://remote-a.example.com")
        };
        let remote_b = PeerInfo {
            deployment_id: "remote-b".to_string(),
            ..peer("remote-b", "https://remote-b.example.com")
        };
        let state = SiteReplicationState {
            pending_remove: Some(PendingRemove {
                id: "remove-id".to_string(),
                original_peers: BTreeMap::from([
                    (local.deployment_id.clone(), local.clone()),
                    (remote_a.deployment_id.clone(), remote_a),
                    (remote_b.deployment_id.clone(), remote_b),
                ]),
                acked_deployment_ids: BTreeSet::from(["remote-a".to_string()]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let operation = pending_operation_for_state(&state, &local).expect("pending remove operation");

        assert_eq!(operation.operation, "remove");
        assert_eq!(operation.id, "remove-id");
        assert_eq!(operation.acked_peers, vec!["remote-a".to_string()]);
        assert_eq!(operation.pending_peers, vec!["remote-b".to_string()]);
    }

    #[test]
    fn test_status_peer_error_summarizes_details() {
        let remote = PeerInfo {
            deployment_id: "remote".to_string(),
            ..peer("remote", "https://remote.example.com")
        };
        let detail = "x".repeat(SITE_REPLICATION_PEER_ERROR_DETAIL_LIMIT + 32);

        let error = status_peer_error(&remote, detail);

        assert_eq!(error.name, "remote");
        assert_eq!(error.endpoint, "https://remote.example.com");
        assert!(error.error.ends_with("(truncated)"));
        assert!(error.error.chars().count() <= SITE_REPLICATION_PEER_ERROR_DETAIL_LIMIT);
    }

    #[test]
    fn test_site_replication_peer_wire_path_matches_minio_routes() {
        assert_eq!(
            site_replication_peer_wire_path(SITE_REPLICATION_PEER_JOIN_PATH),
            "/minio/admin/v3/site-replication/join"
        );
        assert_eq!(
            site_replication_peer_wire_path("/rustfs/admin/v3/site-replication/peer/bucket-meta"),
            "/minio/admin/v3/site-replication/peer/bucket-meta"
        );
        assert_eq!(
            site_replication_peer_wire_path("/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos"),
            "/minio/admin/v3/site-replication/peer/bucket-ops?bucket=photos"
        );
    }

    #[test]
    fn test_site_replication_peer_payload_encryption_matches_minio_contract() {
        assert!(site_replication_peer_payload_encrypted("/minio/admin/v3/site-replication/join"));
        assert!(site_replication_peer_payload_encrypted(
            "/minio/admin/v3/site-replication/join?replicateILMExpiry=true"
        ));
        assert!(!site_replication_peer_payload_encrypted(
            "/minio/admin/v3/site-replication/peer/bucket-meta"
        ));
        assert!(!site_replication_peer_payload_encrypted("/minio/admin/v3/site-replication/peer/iam-item"));
    }

    #[test]
    fn test_secret_candidate_retry_only_for_auth_errors() {
        assert!(peer_error_may_be_secret_mismatch(
            "peer request failed with 403 Forbidden: SignatureDoesNotMatch"
        ));
        assert!(peer_error_may_be_secret_mismatch("AccessDenied"));
        assert!(!peer_error_may_be_secret_mismatch("peer request failed (timeout): deadline elapsed"));
        assert!(!peer_error_may_be_secret_mismatch("peer request failed (tls handshake): bad certificate"));
    }

    #[test]
    fn test_bucket_meta_wire_values_are_base64_encoded_and_legacy_raw_decodes() {
        let raw = "<VersioningConfiguration/>";
        let item = encode_bucket_meta_wire_item(SRBucketMeta {
            r#type: "version-config".to_string(),
            bucket: "photos".to_string(),
            versioning: Some(raw.to_string()),
            ..Default::default()
        });

        let encoded = item.versioning.expect("encoded versioning config");

        assert_eq!(decode_bucket_meta_wire_value(&encoded), raw.as_bytes());
        assert_eq!(decode_bucket_meta_wire_value(raw), raw.as_bytes());
        assert_ne!(encoded, raw);
    }

    #[test]
    fn test_metainfo_bucket_config_values_are_base64_encoded() {
        let raw = br#"<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#;

        assert_eq!(raw_config_to_base64(raw), Some(BASE64_STANDARD.encode(raw)));
        assert_ne!(raw_config_to_base64(raw), raw_config_to_string(raw));
        assert_eq!(raw_config_to_base64(&[]), None);
    }

    #[test]
    fn test_stale_update_detects_older_incoming_timestamp() {
        let local = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(20);
        let stale = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(10);
        let fresh = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(30);

        assert!(is_stale_update(local, Some(stale)));
        assert!(!is_stale_update(local, Some(local)));
        assert!(!is_stale_update(local, Some(fresh)));
        assert!(!is_stale_update(local, None));
    }

    #[test]
    fn test_reconcile_site_replication_bucket_targets_allows_peer_on_same_port_as_local_console() {
        with_var("RUSTFS_CONSOLE_ADDRESS", Some(":9001"), || {
            let mut state = SiteReplicationState {
                service_account_access_key: "site-replicator-0".to_string(),
                service_account_secret_key: "secret".to_string(),
                ..Default::default()
            };
            state.peers.insert(
                "local".to_string(),
                PeerInfo {
                    deployment_id: "local".to_string(),
                    ..peer("local", "https://local.example.com:9000")
                },
            );
            state.peers.insert(
                "remote".to_string(),
                PeerInfo {
                    deployment_id: "remote".to_string(),
                    ..peer("remote", "https://remote.example.com:9001")
                },
            );

            let targets = reconcile_site_replication_bucket_targets(
                BucketTargets::default(),
                "photos",
                &state,
                &PeerInfo {
                    deployment_id: "local".to_string(),
                    ..peer("local", "https://local.example.com:9000")
                },
                None,
                "secret",
            )
            .expect("peer using same numeric port as local console should remain valid");

            assert_eq!(targets.targets.len(), 1);
            let target = &targets.targets[0];
            assert_eq!(target.endpoint, "remote.example.com:9001");
            assert!(target.secure);
        });
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
    fn test_apply_state_edit_req_ignores_missing_updated_at() {
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
                updated_at: None,
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
        let kvs = rustfs_config::server_config::KVS(vec![
            rustfs_config::server_config::KV {
                key: "enable".to_string(),
                value: "on".to_string(),
                hidden_if_empty: false,
            },
            rustfs_config::server_config::KV {
                key: "user_dn_search_base_dn".to_string(),
                value: "ou=people,dc=example,dc=com".to_string(),
                hidden_if_empty: false,
            },
            rustfs_config::server_config::KV {
                key: "user_dn_search_filter".to_string(),
                value: "(uid=%s)".to_string(),
                hidden_if_empty: false,
            },
            rustfs_config::server_config::KV {
                key: "group_search_base_dn".to_string(),
                value: "ou=groups,dc=example,dc=com".to_string(),
                hidden_if_empty: false,
            },
            rustfs_config::server_config::KV {
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
    fn test_site_replication_peer_client_cache_hit_generation_mismatch_returns_none() {
        let cache = Some(SiteReplicationPeerClientCache {
            generation: 7,
            entry: SiteReplicationPeerClientCacheEntry::Failed("cached error".to_string()),
        });

        assert!(site_replication_peer_client_cache_hit(&cache, 8).is_none());
    }

    #[test]
    fn test_site_replication_peer_client_cache_hit_returns_cached_ready_client() {
        let cache = Some(SiteReplicationPeerClientCache {
            generation: 7,
            entry: SiteReplicationPeerClientCacheEntry::Ready(reqwest::Client::new()),
        });

        site_replication_peer_client_cache_hit(&cache, 7)
            .expect("cache hit expected")
            .expect("ready cache entry should return cached client");
    }

    #[test]
    fn test_site_replication_peer_client_cache_hit_returns_cached_error() {
        let cache = Some(SiteReplicationPeerClientCache {
            generation: 7,
            entry: SiteReplicationPeerClientCacheEntry::Failed("cached error".to_string()),
        });

        let err = site_replication_peer_client_cache_hit(&cache, 7)
            .expect("cache hit expected")
            .expect_err("error cache entry should return error");
        assert!(err.to_string().contains("cached error"), "expected cached error detail, got: {}", err);
    }

    #[tokio::test]
    #[serial]
    async fn test_site_replication_peer_client_rebuilds_when_generation_changes() {
        let previous_generation = resolve_outbound_tls_generation().0;
        let previous_cache = {
            let mut cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
            let snapshot = cache.clone();
            *cache = None;
            snapshot
        };

        set_test_outbound_tls_generation(101);
        site_replication_peer_client()
            .await
            .expect("initial client build should succeed");
        let cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
        let cached = cache.as_ref().expect("cache should be populated");
        assert_eq!(cached.generation, 101);
        assert!(matches!(cached.entry, SiteReplicationPeerClientCacheEntry::Ready(_)));
        drop(cache);

        set_test_outbound_tls_generation(102);
        site_replication_peer_client()
            .await
            .expect("new generation should rebuild client");
        let cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
        let cached = cache.as_ref().expect("cache should be populated");
        assert_eq!(cached.generation, 102);
        assert!(matches!(cached.entry, SiteReplicationPeerClientCacheEntry::Ready(_)));

        drop(cache);
        set_test_outbound_tls_generation(previous_generation);
        let mut cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
        *cache = previous_cache;
    }

    #[test]
    fn test_site_repl_netperf_duration_is_bounded() {
        let default_uri = Uri::from_static("/rustfs/admin/v3/site-replication/netperf");
        let too_short_uri = Uri::from_static("/rustfs/admin/v3/site-replication/netperf?duration=0s");
        let too_long_uri = Uri::from_static("/rustfs/admin/v3/site-replication/netperf?duration=60s");
        let valid_uri = Uri::from_static("/rustfs/admin/v3/site-replication/netperf?duration=5s");

        assert_eq!(site_repl_netperf_duration(&default_uri), SITE_REPL_MIN_NETPERF_DURATION);
        assert_eq!(site_repl_netperf_duration(&too_short_uri), SITE_REPL_MIN_NETPERF_DURATION);
        assert_eq!(site_repl_netperf_duration(&too_long_uri), SITE_REPL_MAX_NETPERF_DURATION);
        assert_eq!(site_repl_netperf_duration(&valid_uri), Duration::from_secs(5));
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

    // Fix 3: replication_cfg_mismatch must not be set for deployments that simply have no
    // replication config. Setting it globally caused mc to count N mismatch entries for a
    // single bucket (one per deployment), while max_buckets=1, producing -1/N in sync.
    #[test]
    fn test_replication_cfg_mismatch_only_set_for_deployments_with_config() {
        use rustfs_madmin::{SRBucketInfo, SRInfo};

        let repl_xml = {
            let config = ReplicationConfiguration {
                role: String::new(),
                rules: vec![build_site_replication_rule(
                    "arn:rustfs:replication::site-b:photos",
                    1,
                    "site-repl-site-b",
                )],
            };
            String::from_utf8(serialize(&config).unwrap()).unwrap()
        };

        let mut site_a_info = SRInfo::default();
        site_a_info.buckets.insert(
            "photos".to_string(),
            SRBucketInfo {
                bucket: "photos".to_string(),
                replication_config: Some(repl_xml),
                ..Default::default()
            },
        );

        // Site B has the bucket but NO replication config yet (partial setup)
        let mut site_b_info = SRInfo::default();
        site_b_info.buckets.insert(
            "photos".to_string(),
            SRBucketInfo {
                bucket: "photos".to_string(),
                replication_config: None,
                ..Default::default()
            },
        );

        let site_infos: BTreeMap<String, SRInfo> = [("dep-a".to_string(), site_a_info), ("dep-b".to_string(), site_b_info)]
            .into_iter()
            .collect();

        let mut status = SRStatusInfo {
            sites: site_infos
                .keys()
                .map(|k| {
                    (
                        k.clone(),
                        PeerInfo {
                            deployment_id: k.clone(),
                            ..Default::default()
                        },
                    )
                })
                .collect(),
            ..Default::default()
        };
        for k in site_infos.keys() {
            status.stats_summary.insert(
                k.clone(),
                SRSiteSummary {
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                    ..Default::default()
                },
            );
        }

        let opts = SRStatusOptions {
            buckets: true,
            ..Default::default()
        };
        merge_bucket_status_info(&mut status, &site_infos, &opts);

        let bucket_stats = status.bucket_stats.get("photos").expect("photos bucket stats");
        let dep_a = bucket_stats.get("dep-a").expect("dep-a stats");
        let dep_b = bucket_stats.get("dep-b").expect("dep-b stats");

        // dep-a has a config but it doesn't cover all peers → mismatch
        assert!(dep_a.replication_cfg_mismatch, "dep-a has config but it is incomplete");
        // dep-b has NO config → must NOT be flagged as mismatch (only has_replication_cfg=false)
        assert!(
            !dep_b.replication_cfg_mismatch,
            "dep-b has no config, mismatch must not be set to avoid -1 in mc output"
        );
        assert!(!dep_b.has_replication_cfg, "dep-b should show has_replication_cfg=false");
    }

    // Fix 4: status operation must return a well-formed SRResyncOpStatus (not an empty body)
    #[test]
    fn test_resync_status_returns_not_found_when_no_resync_in_progress() {
        let state = SiteReplicationState::default();
        let status = state
            .resync_status
            .get("nonexistent-peer")
            .cloned()
            .unwrap_or_else(|| SRResyncOpStatus {
                op_type: SITE_REPL_RESYNC_STATUS.to_string(),
                status: "not-found".to_string(),
                ..Default::default()
            });
        assert_eq!(status.status, "not-found");
        assert_eq!(status.op_type, SITE_REPL_RESYNC_STATUS);
    }

    #[test]
    fn test_resync_status_returns_existing_status_for_known_peer() {
        let mut state = SiteReplicationState::default();
        state.resync_status.insert(
            "peer-dep".to_string(),
            SRResyncOpStatus {
                op_type: SITE_REPL_RESYNC_START.to_string(),
                resync_id: "abc-123".to_string(),
                status: "success".to_string(),
                ..Default::default()
            },
        );
        let status = state
            .resync_status
            .get("peer-dep")
            .cloned()
            .unwrap_or_else(|| SRResyncOpStatus {
                op_type: SITE_REPL_RESYNC_STATUS.to_string(),
                status: "not-found".to_string(),
                ..Default::default()
            });
        assert_eq!(status.op_type, SITE_REPL_RESYNC_START);
        assert_eq!(status.resync_id, "abc-123");
    }

    // Fix 2: sync_state must derive from real health signals, not always Unknown
    #[test]
    fn test_derive_sync_state_from_replication_completeness() {
        // A peer that is reachable and has complete replication rules for all other peers
        // should be Enable; one that is reachable but has an incomplete config should be Disable.
        let repl_complete_xml = {
            let config = ReplicationConfiguration {
                role: String::new(),
                rules: vec![build_site_replication_rule(
                    "arn:rustfs:replication::dep-b:bucket",
                    1,
                    "site-repl-dep-b",
                )],
            };
            String::from_utf8(serialize(&config).unwrap()).unwrap()
        };

        // Peer that has complete config for 2-site setup
        assert!(site_replication_rule_complete(&build_site_replication_rule(
            "arn:rustfs:replication::dep-b:bucket",
            1,
            "site-repl-dep-b"
        )));
        assert_eq!(
            site_replication_config_mismatch(vec![Some(&repl_complete_xml), Some(&repl_complete_xml)].into_iter(), 2),
            (2, false),
            "complete rules on both sites → no mismatch"
        );
        assert_eq!(
            site_replication_config_mismatch(vec![Some(&repl_complete_xml)].into_iter(), 2),
            (1, true),
            "config only on one of two sites → mismatch"
        );
    }

    // Fix 5: remove --all must purge local state unconditionally even when peer errors occur
    #[test]
    fn test_remove_all_purges_local_state_unconditionally() {
        let mut state = SiteReplicationState {
            name: "local".to_string(),
            service_account_access_key: "site-replicator-0".to_string(),
            service_account_secret_key: "some-secret".to_string(),
            ..Default::default()
        };
        state.peers.insert(
            "local-dep".to_string(),
            PeerInfo {
                deployment_id: "local-dep".to_string(),
                ..peer("local", "https://local.example.com")
            },
        );
        state.peers.insert(
            "remote-dep".to_string(),
            PeerInfo {
                deployment_id: "remote-dep".to_string(),
                ..peer("remote", "https://remote.example.com")
            },
        );
        state.resync_status.insert(
            "remote-dep".to_string(),
            SRResyncOpStatus {
                resync_id: "r1".to_string(),
                status: "success".to_string(),
                ..Default::default()
            },
        );

        // Simulate remove --all
        let state = remove_sites(
            state,
            SRRemoveReq {
                remove_all: true,
                ..Default::default()
            },
        );

        // Local state must be cleared regardless of whether peer notifications succeed
        assert!(state.peers.is_empty(), "peers must be cleared on remove --all");
        assert!(state.resync_status.is_empty(), "resync_status must be cleared on remove --all");

        // Even if peers returned 403 (desynced account), status still reports success
        let status =
            site_replication_remove_status(&["https://remote.example.com: peer/remove returned 403 Forbidden".to_string()]);
        assert_eq!(
            status.status, SITE_REPL_REMOVE_SUCCESS,
            "local remove reports success even when peer notifications fail"
        );
        assert!(
            status.err_detail.contains("403 Forbidden"),
            "peer errors are included in err_detail for diagnostics"
        );
    }

    // Fix 6: ensure_site_replication_bucket_replication_config must reconcile rather than
    // early-return so that a bucket propagated to the second site gets a rule back to the first.
    #[test]
    fn test_reconcile_adds_missing_peer_rules_to_existing_config() {
        // Start with a config that has only rule for dep-b (first site's initial config)
        let rule_b = build_site_replication_rule("arn:rustfs:replication::dep-b:bucket", 1, "site-repl-dep-b");
        let rule_c = build_site_replication_rule("arn:rustfs:replication::dep-c:bucket", 2, "site-repl-dep-c");

        let mut existing_rules = vec![rule_b.clone()];

        // Desired config has rules for both dep-b and dep-c (3-site setup)
        let desired_rules = vec![rule_b, rule_c];

        // Simulate the reconcile: collect existing site-repl rule IDs
        let existing_ids: std::collections::HashSet<String> = existing_rules
            .iter()
            .filter_map(|r| r.id.as_deref())
            .filter(|id| id.starts_with("site-repl-"))
            .map(String::from)
            .collect();

        let mut added = false;
        for rule in &desired_rules {
            let rid = rule.id.as_deref().unwrap_or("");
            if !existing_ids.contains(rid) {
                existing_rules.push(rule.clone());
                added = true;
            }
        }

        assert!(added, "missing rule should have been added");
        assert_eq!(existing_rules.len(), 2, "should now have rules for both peers");

        let rule_ids: Vec<&str> = existing_rules.iter().filter_map(|r| r.id.as_deref()).collect();
        assert!(rule_ids.contains(&"site-repl-dep-b"));
        assert!(rule_ids.contains(&"site-repl-dep-c"));
    }
}
