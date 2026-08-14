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

use crate::admin::auth::authorize_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::{
    current_deployment_id, current_endpoints_handle, current_federated_identity_service, current_iam_handle,
    current_object_store_handle, current_outbound_tls_generation, current_outbound_tls_state, current_region,
    current_replication_pool_handle, current_replication_stats_handle, current_runtime_port, current_server_config,
    current_token_signing_key, object_store_from_req,
};
use crate::admin::site_replication_identity::{
    canonical_endpoint, deployment_id_for_endpoint, is_https_endpoint, mark_unknown_peer_sync_enabled,
    normalize_peer_map_by_identity_with, same_identity_endpoint, site_identity_key,
};
use crate::admin::storage_api::bucket::metadata::{
    BUCKET_CORS_CONFIG, BUCKET_LIFECYCLE_CONFIG, BUCKET_POLICY_CONFIG, BUCKET_QUOTA_CONFIG_FILE, BUCKET_REPLICATION_CONFIG,
    BUCKET_SSECONFIG, BUCKET_TAGGING_CONFIG, BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG, OBJECT_LOCK_CONFIG,
};
use crate::admin::storage_api::bucket::metadata_sys;
use crate::admin::storage_api::bucket::replication;
use crate::admin::storage_api::bucket::target::{ARN, BucketTarget, BucketTargetType, BucketTargets, Credentials};
use crate::admin::storage_api::bucket::target_sys::BucketTargetSys;
use crate::admin::storage_api::bucket::utils::{deserialize, serialize};
use crate::admin::storage_api::bucket::{AdminReplicationConfigExt as _, AdminVersioningConfigExt as _};
use crate::admin::storage_api::config::read_admin_config;
#[cfg(test)]
use crate::admin::storage_api::config::save_admin_config;
use crate::admin::storage_api::contract::bucket::{
    BucketOperations, BucketOptions, DeleteBucketOptions, MakeBucketOptions, SRBucketDeleteOp,
};
use crate::admin::storage_api::error::Error as StorageError;
use crate::admin::storage_api::runtime::ECStore;
use crate::admin::utils::{encode_compatible_admin_payload, read_compatible_admin_body};
use crate::auth::constant_time_eq;
use crate::config::get_config_snapshot;
use crate::error::ApiError;
use crate::server::ADMIN_PREFIX;
use crate::storage::storage_api::{
    delete_config_no_lock, lock_bucket_targets_metadata, read_config_no_lock, save_config_no_lock, with_config_object_read_lock,
    with_config_object_write_lock,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use http::header::{CONTENT_TYPE, HOST};
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_config::{
    DEFAULT_CONSOLE_ADDRESS, DEFAULT_DELIMITER, DEFAULT_RUSTFS_TLS_PATH, ENV_RUSTFS_CONSOLE_ADDRESS, ENV_RUSTFS_TLS_PATH,
    MAX_ADMIN_REQUEST_BODY_SIZE,
};
use rustfs_iam::error::is_err_no_such_service_account;
use rustfs_iam::federation::OIDC_VIRTUAL_PARENT_CLAIM;
use rustfs_iam::store::{MappedPolicy, UserType, sr_wire_user_type, user_type_from_sr_wire};
use rustfs_iam::sys::{
    NewServiceAccountOpts, SITE_REPLICATOR_SERVICE_ACCOUNT, UpdateServiceAccountOpts, get_claims_from_token_with_secret,
};
use rustfs_madmin::{
    AddOrUpdateUserReq, BucketBandwidth, GroupAddRemove, GroupStatus, IDPSettings, InProgressMetric, InQueueMetric,
    LDAPConfigSettings, LDAPSettings, OpenIDProviderSettings, PeerInfo, PeerSite, QStat, ReplProxyMetric, ReplicateAddStatus,
    ReplicateEditStatus, ReplicateRemoveStatus, ResyncBucketStatus, SITE_REPL_API_VERSION, SR_IAM_ITEM_STS_ACC,
    SR_IAM_ITEM_STS_ACC_LEGACY, SRBucketInfo, SRBucketMeta, SRBucketStatsSummary, SRGroupInfo, SRGroupStatsSummary, SRIAMItem,
    SRIAMPolicy, SRILMExpiryStatsSummary, SRInfo, SRMetric, SRMetricsSummary, SRPeerError, SRPeerJoinReq, SRPendingOperation,
    SRPolicyMapping, SRPolicyStatsSummary, SRRemoveReq, SRResyncOpStatus, SRRetryStats, SRSessionPolicy, SRSiteSummary,
    SRStateEditReq, SRStateInfo, SRStatusInfo, SRSvcAccCreate, SRUserStatsSummary, SiteReplicationInfo, SyncStatus, WorkerStat,
};
use rustfs_policy::policy::{
    Policy,
    action::{Action, AdminAction},
};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use rustfs_tls_runtime::GlobalPublishedOutboundTlsState;
use rustfs_utils::egress::{OutboundUrlError, validate_outbound_url};
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
use serde::de::{DeserializeOwned, IgnoredAny};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};
use url::{Url, form_urlencoded};
use uuid::Uuid;

const LOG_COMPONENT_ADMIN: &str = "admin";
const LOG_SUBSYSTEM_SITE_REPLICATION: &str = "site_replication";
const EVENT_ADMIN_SITE_REPLICATION_STATE: &str = "admin_site_replication_state";
const SERVICE_ACCOUNT_ENVELOPE_VERSION: u64 = 2;
use crate::admin::site_replication_state::{SITE_REPLICATION_STATE_PATH, with_site_replication_state_lock};
const SITE_REPLICATION_REPAIR_STATE_PATH: &str = "config/site-replication/repair-state.json";
const SITE_REPLICATION_REPAIR_EXECUTION_LOCK_PATH: &str = "config/site-replication/repair-execution.lock";
const SITE_REPL_ADD_SUCCESS: &str = "Requested sites were configured for replication successfully.";
const SITE_REPL_EDIT_SUCCESS: &str = "Requested site was updated successfully.";
const SITE_REPL_REMOVE_SUCCESS: &str = "Requested site(s) were removed from cluster replication successfully.";
const SITE_REPL_RESYNC_START: &str = "start";
const SITE_REPL_RESYNC_CANCEL: &str = "cancel";
const SITE_REPL_RESYNC_STATUS: &str = "status";
const SITE_REPL_RESYNC_DEFAULT_PAGE_SIZE: usize = 100;
const SITE_REPL_RESYNC_MAX_PAGE_SIZE: usize = 1000;
const SITE_REPLICATION_PEER_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SITE_REPLICATION_PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const SITE_REPLICATION_PEER_ERROR_DETAIL_LIMIT: usize = 256;
const SITE_REPLICATION_INITIAL_SYNC_ERROR_LIMIT: usize = 32;
const MAX_PEER_CA_CERT_PEM_SIZE: usize = 256 * 1024;
const ALLOW_LOOPBACK_REPLICATION_TARGET_ENV: &str = "RUSTFS_REPLICATION_ALLOW_LOOPBACK_TARGET";
const SITE_REPLICATION_RETRY_QUEUE_LIMIT: usize = 256;
const SITE_REPLICATION_RETRY_FAILED_AFTER: u32 = 3;
const SITE_REPLICATION_REPAIR_OPERATION_LIMIT: usize = 32;
const SITE_REPLICATION_REPAIR_IAM_FAMILY: &str = "iam";
const SITE_REPLICATION_REPAIR_BUCKET_FAMILY: &str = "bucket";
const SITE_REPLICATION_REPAIR_BUCKET_METADATA_FAMILY: &str = "bucket-metadata";
const SITE_REPLICATION_REPAIR_REPLICATION_FAMILY: &str = "replication";
const SITE_REPLICATION_PEER_BUCKET_OPS_PATH: &str = "/rustfs/admin/v3/site-replication/peer/bucket-ops";
const SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING: &str = "make-with-versioning";
const SITE_REPLICATION_BUCKET_OP_CONFIGURE_REPLICATION: &str = "configure-replication";
const IDENTITY_LDAP_SUB_SYS: &str = "identity_ldap";
const LEGACY_LDAP_SUB_SYS: &str = "ldapserverconfig";
const SITE_REPLICATION_PEER_JOIN_PATH: &str = "/rustfs/admin/v3/site-replication/peer/join";
const SITE_REPLICATION_PEER_EDIT_PATH: &str = "/rustfs/admin/v3/site-replication/peer/edit";
const SITE_REPLICATION_PEER_EDIT_CAPABILITY_PATH: &str =
    "/rustfs/admin/v3/site-replication/peer/edit-capabilities?capability=endpoint-target-refresh";
const SITE_REPLICATION_PEER_TLS_CAPABILITY_PATH: &str =
    "/rustfs/admin/v3/site-replication/peer/edit-capabilities?capability=peer-tls-settings";
const SITE_REPLICATION_PEER_EDIT_REFRESH_PATH: &str = "/rustfs/admin/v3/site-replication/peer/edit?refresh-targets=true";
/// Peer-edit fencing token, carried as query parameters so a peer that predates
/// the fence simply ignores them (unknown query keys are dropped) and keeps the
/// previous last-writer-wins behaviour.
const SITE_REPLICATION_EDIT_ORIGIN_QUERY: &str = "editOrigin";
const SITE_REPLICATION_EDIT_GENERATION_QUERY: &str = "editGeneration";
const SITE_REPLICATION_ENDPOINT_REFRESH_RETRY_PATH: &str = "internal:endpoint-target-refresh";
const SITE_REPLICATION_PEER_REMOVE_PATH: &str = "/rustfs/admin/v3/site-replication/peer/remove";
const SITE_REPLICATION_DEVNULL_PATH: &str = "/rustfs/admin/v3/site-replication/devnull";
const RUSTFS_ADMIN_V3_PREFIX: &str = "/rustfs/admin/v3";
const MINIO_ADMIN_V3_PREFIX: &str = "/minio/admin/v3";
const MINIO_SITE_REPLICATION_PEER_JOIN_PATH: &str = "/minio/admin/v3/site-replication/peer/join";

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
        "s3:DeleteObjectVersionTagging",
        "s3:GetObjectRetention",
        "s3:PutObjectRetention",
        "s3:GetObjectLegalHold",
        "s3:PutObjectLegalHold"
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct PeerConnection {
    endpoint: Url,
    skip_tls_verify: bool,
    ca_cert_pem: String,
}

#[derive(Deserialize, Default)]
struct PeerTlsFieldPresence {
    #[serde(rename = "skipTlsVerify")]
    skip_tls_verify: Option<IgnoredAny>,
    #[serde(rename = "caCertPem")]
    ca_cert_pem: Option<IgnoredAny>,
}

impl PeerTlsFieldPresence {
    fn has_skip_tls_verify(&self) -> bool {
        self.skip_tls_verify.is_some()
    }

    fn has_ca_cert_pem(&self) -> bool {
        self.ca_cert_pem.is_some()
    }
}

#[derive(Clone)]
struct PeerDnsResolver {
    allow_loopback: bool,
    #[cfg(test)]
    overrides: Option<Arc<HashMap<String, Vec<IpAddr>>>>,
}

impl PeerDnsResolver {
    fn new(allow_loopback: bool) -> Self {
        Self {
            allow_loopback,
            #[cfg(test)]
            overrides: None,
        }
    }

    #[cfg(test)]
    fn with_overrides(allow_loopback: bool, overrides: HashMap<String, Vec<IpAddr>>) -> Self {
        Self {
            allow_loopback,
            overrides: Some(Arc::new(overrides)),
        }
    }
}

impl reqwest::dns::Resolve for PeerDnsResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        let allow_loopback = self.allow_loopback;
        #[cfg(test)]
        let overrides = self.overrides.clone();
        Box::pin(async move {
            #[cfg(test)]
            let overridden = overrides.as_ref().and_then(|entries| entries.get(&host)).cloned();
            #[cfg(not(test))]
            let overridden: Option<Vec<IpAddr>> = None;

            let ips = if let Some(ips) = overridden {
                ips
            } else {
                tokio::net::lookup_host((host.as_str(), 0))
                    .await?
                    .map(|addr| addr.ip())
                    .collect()
            };
            let addrs = ips
                .into_iter()
                .filter(|ip| resolved_peer_ip_allowed(&host, *ip, allow_loopback))
                .map(|ip| SocketAddr::new(ip, 0))
                .collect::<Vec<_>>();
            if addrs.is_empty() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    format!("site replication DNS resolution for `{host}` returned no allowed addresses"),
                )
                .into());
            }
            Ok(Box::new(addrs.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

impl PeerConnection {
    fn new(endpoint: &str, skip_tls_verify: bool, ca_cert_pem: &str) -> S3Result<Self> {
        validate_peer_connection_inner(endpoint, skip_tls_verify, ca_cert_pem, loopback_replication_targets_allowed())
    }

    fn endpoint(&self) -> &str {
        self.endpoint.as_str().trim_end_matches('/')
    }

    fn uses_default_tls(&self) -> bool {
        !self.skip_tls_verify && self.ca_cert_pem.is_empty()
    }
}

impl TryFrom<&PeerInfo> for PeerConnection {
    type Error = S3Error;

    fn try_from(peer: &PeerInfo) -> Result<Self, Self::Error> {
        Self::new(&peer.endpoint, peer.skip_tls_verify, &peer.ca_cert_pem)
    }
}

impl TryFrom<&PeerSite> for PeerConnection {
    type Error = S3Error;

    fn try_from(site: &PeerSite) -> Result<Self, Self::Error> {
        Self::new(&site.endpoint, site.skip_tls_verify, &site.ca_cert_pem)
    }
}

static SITE_REPLICATION_PEER_CLIENT: LazyLock<Mutex<Option<SiteReplicationPeerClientCache>>> = LazyLock::new(|| Mutex::new(None));
// Lock order: lifecycle -> bucket operation -> repair admission -> state -> per-bucket metadata.
// "state" is the distributed state-object lock in
// crate::admin::site_replication_state, entered through
// update_site_replication_state (P1-15). There is no process-local state
// mutex any more: it could not order two nodes of one site, and the call
// sites that needed ordering carry a generation fence instead.
static SITE_REPLICATION_LIFECYCLE_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
static SITE_REPLICATION_BUCKET_OP_LOCK: LazyLock<RwLock<()>> = LazyLock::new(|| RwLock::new(()));
static SITE_REPLICATION_ADD_BOOTSTRAP: LazyLock<StdMutex<Option<SiteReplicationAddBootstrap>>> =
    LazyLock::new(|| StdMutex::new(None));

struct SiteReplicationAddBootstrap {
    token: Uuid,
    buckets: HashSet<String>,
}

struct SiteReplicationAddInProgressGuard {
    token: Uuid,
    _lifecycle: SiteReplicationLifecycleGuard,
}

struct SiteReplicationLifecycleGuard {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl SiteReplicationLifecycleGuard {
    async fn acquire() -> Self {
        Self {
            _guard: SITE_REPLICATION_LIFECYCLE_LOCK.lock().await,
        }
    }

    /// Non-blocking variant for background work that must never interleave with an
    /// add/remove/endpoint-refresh: those run in phases, and rebuilding rules between two of
    /// them would resurrect exactly what the operation just tore down. Skipping a round is
    /// free — the next tick picks it up.
    fn try_acquire() -> Option<Self> {
        SITE_REPLICATION_LIFECYCLE_LOCK
            .try_lock()
            .ok()
            .map(|guard| Self { _guard: guard })
    }
}

impl SiteReplicationAddInProgressGuard {
    fn start(lifecycle: SiteReplicationLifecycleGuard, buckets: HashSet<String>) -> S3Result<Self> {
        let token = Uuid::new_v4();
        let mut pending = SITE_REPLICATION_ADD_BOOTSTRAP.lock().map_err(|_| {
            S3Error::with_message(S3ErrorCode::InternalError, "site replication bootstrap lock poisoned".to_string())
        })?;
        *pending = Some(SiteReplicationAddBootstrap { token, buckets });
        Ok(Self {
            token,
            _lifecycle: lifecycle,
        })
    }
}

impl Drop for SiteReplicationAddInProgressGuard {
    fn drop(&mut self) {
        if let Ok(mut pending) = SITE_REPLICATION_ADD_BOOTSTRAP.lock()
            && pending.as_ref().is_some_and(|bootstrap| bootstrap.token == self.token)
        {
            *pending = None;
        }
    }
}

fn bootstrap_peer_bucket_operation_allowed(bucket: &str, operation: &str, bootstrap_token: Option<&str>) -> bool {
    if !matches!(operation, "make-with-versioning" | "configure-replication") {
        return false;
    }
    let parsed_token = bootstrap_token.and_then(|value| Uuid::parse_str(value).ok());
    SITE_REPLICATION_ADD_BOOTSTRAP.lock().is_ok_and(|pending| {
        pending.as_ref().is_some_and(|bootstrap| {
            parsed_token.is_some_and(|token| token == bootstrap.token)
                || (bootstrap_token.is_none() && bootstrap.buckets.contains(bucket))
        })
    })
}

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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_endpoint_refresh: Option<PendingEndpointRefresh>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    retry_queue: Vec<SiteReplicationRetryEvent>,
    #[serde(default)]
    sync_state_initialized: bool,
    /// Fencing token for peer-edit delivery, allocated inside the state
    /// transaction (the distributed state-object lock). Two nodes of THIS
    /// site that accept admin edits concurrently therefore get strictly
    /// ordered generations, and a delivery that stalls can be recognised as
    /// stale by the receiving site.
    #[serde(default)]
    edit_generation: u64,
    /// Per-origin high-water mark of the peer edits already applied here,
    /// keyed by the origin site's deployment id. A delivery whose generation
    /// is not above the mark arrived out of order and must not overwrite the
    /// newer edit that already landed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    applied_edit_generations: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SiteReplicationRepairState {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    operations: BTreeMap<String, SiteReplicationRepairOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SiteReplicationRepairOperation {
    operation_id: String,
    preflight_token: String,
    plan_token: String,
    status: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    sites: BTreeMap<String, SiteReplicationRepairSiteStatus>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    created_at: Option<OffsetDateTime>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    updated_at: Option<OffsetDateTime>,
    #[serde(default, with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SiteReplicationRepairSiteStatus {
    deployment_id: String,
    name: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    families: BTreeMap<String, SiteReplicationRepairFamilyStatus>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SiteReplicationRepairFamilyStatus {
    planned: usize,
    succeeded: usize,
    failed: usize,
    #[serde(default)]
    retry_events: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    tasks: Vec<SiteReplicationRepairTaskStatus>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    errors: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
struct SiteReplicationRepairTaskStatus {
    task_id: String,
    status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SiteReplicationRepairRequest {
    mode: SiteReplicationRepairMode,
    #[serde(default)]
    preflight_token: Option<String>,
    #[serde(default)]
    operation_id: Option<String>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum SiteReplicationRepairMode {
    DryRun,
    Execute,
}

struct SiteReplicationRepairExecutionRequest {
    local_peer: PeerInfo,
    preflight_token: String,
    operation_id: String,
    signing_key: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SiteReplicationRepairPreflight {
    mode: &'static str,
    status: &'static str,
    preflight_token: String,
    retry_events: usize,
    sites: BTreeMap<String, SiteReplicationRepairSiteStatus>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SiteReplicationRepairOperationResponse {
    mode: &'static str,
    operation_id: String,
    status: String,
    sites: BTreeMap<String, SiteReplicationRepairSiteResponse>,
    #[serde(with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    created_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    updated_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option", skip_serializing_if = "Option::is_none")]
    completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SiteReplicationRepairSiteResponse {
    deployment_id: String,
    name: String,
    families: BTreeMap<String, SiteReplicationRepairFamilyResponse>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SiteReplicationRepairFamilyResponse {
    planned: usize,
    succeeded: usize,
    failed: usize,
    retry_events: usize,
    tasks: Vec<SiteReplicationRepairTaskStatus>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<String>,
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
    /// Peer-edit generation whose delivery failed, when the failing send
    /// carried one. Settling a *later* success for the same (peer, path) must
    /// not erase a failure recorded for a NEWER generation — see
    /// [`settle_site_replication_retry_events`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    edit_generation: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PendingEndpointRefresh {
    id: String,
    peer: PeerInfo,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    remote_peers: BTreeMap<String, PeerInfo>,
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    acked_deployment_ids: BTreeSet<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EndpointRefreshRequest {
    id: String,
    peer: PeerInfo,
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
    bucket_names: HashSet<String>,
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
    #[serde(rename = "initialSyncErrorMessage", default, skip_serializing_if = "String::is_empty")]
    initial_sync_error_message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SRPeerJoinEnvelope {
    #[serde(flatten)]
    request: SRPeerJoinReq,
    #[serde(rename = "deferSyncStateEnable", default, skip_serializing_if = "std::ops::Not::not")]
    defer_sync_state_enable: bool,
}

#[derive(Debug, Default)]
struct SiteReplicationErrorSummary {
    entries: Vec<String>,
    total: usize,
}

impl SiteReplicationErrorSummary {
    fn push(&mut self, error: impl AsRef<str>) {
        self.total = self.total.saturating_add(1);
        if self.entries.len() < SITE_REPLICATION_INITIAL_SYNC_ERROR_LIMIT {
            self.entries.push(summarize_peer_error_detail(error.as_ref()));
        }
    }

    fn extend(&mut self, other: Self) {
        self.total = self.total.saturating_add(other.total);
        let remaining = SITE_REPLICATION_INITIAL_SYNC_ERROR_LIMIT.saturating_sub(self.entries.len());
        self.entries.extend(other.entries.into_iter().take(remaining));
    }

    fn is_empty(&self) -> bool {
        self.total == 0
    }

    fn reported(&self) -> usize {
        self.entries.len()
    }

    fn render(&self) -> String {
        let mut message = self.entries.join("; ");
        let omitted = self.total.saturating_sub(self.entries.len());
        if omitted > 0 {
            if !message.is_empty() {
                message.push_str("; ");
            }
            message.push_str(&format!("{omitted} additional error(s) omitted"));
        }
        message
    }
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
    // Hand the reconciler to the infra-layer scheduler here rather than letting startup call
    // into this module: startup sits below this layer and must not depend upwards. The admin
    // router is built before startup reconciles, so the hook is always installed in time.
    crate::site_replication_reconcile::register_site_replication_reconciler(reconcile_site_replication_wiring);

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
        (
            Method::PUT,
            "/v3/site-replication/peer/edit-capabilities",
            AdminOperation(&SRPeerEditCapabilitiesHandler {}),
        ),
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
            Method::GET,
            "/v3/site-replication/repair/status",
            AdminOperation(&SiteReplicationRepairStatusHandler {}),
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
    authorize_admin_request(req, vec![Action::AdminAction(action)]).await
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
    let body = read_site_replication_body(req, secret_key, compat_encrypted).await?;
    parse_site_replication_json(&body)
}

async fn read_site_replication_body(req: S3Request<Body>, secret_key: &str, compat_encrypted: bool) -> S3Result<Vec<u8>> {
    let body = if compat_encrypted {
        read_compatible_admin_body(req.input, MAX_ADMIN_REQUEST_BODY_SIZE, req.uri.path(), secret_key).await?
    } else {
        read_plain_admin_body(req.input).await?
    };
    Ok(body)
}

fn parse_site_replication_json<T: DeserializeOwned>(body: &[u8]) -> S3Result<T> {
    serde_json::from_slice(body).map_err(|e| s3_error!(InvalidRequest, "invalid JSON: {}", e))
}

fn parse_public_peer_edit(body: &[u8]) -> S3Result<(PeerInfo, PeerTlsFieldPresence)> {
    Ok((parse_site_replication_json(body)?, parse_site_replication_json(body)?))
}

fn parse_site_replication_state(data: &[u8]) -> S3Result<SiteReplicationState> {
    let mut state: SiteReplicationState = serde_json::from_slice(data)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("invalid site replication state: {e}")))?;
    state.peers = normalize_peer_map_by_identity(state.peers);
    // A peer-edit high-water mark only fences a CURRENT peer. A site that
    // leaves drops below two peers, which clears its own state object and
    // restarts its generation counter at zero — a mark left over from the
    // previous membership would then reject every edit it sends after it
    // rejoins. Dropping departed origins on load also keeps the map bounded.
    state
        .applied_edit_generations
        .retain(|origin, _| state.peers.contains_key(origin));
    if !state.sync_state_initialized {
        if state.enabled() {
            mark_unknown_peer_sync_enabled(&mut state.peers);
        }
        state.sync_state_initialized = true;
    }
    Ok(state)
}

async fn load_site_replication_state() -> S3Result<SiteReplicationState> {
    let Some(store) = current_object_store_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };

    match read_admin_config(store, SITE_REPLICATION_STATE_PATH).await {
        Ok(data) => parse_site_replication_state(&data),
        Err(StorageError::ConfigNotFound) => Ok(SiteReplicationState::default()),
        Err(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to load site replication state: {err}"),
        )),
    }
}

async fn load_site_replication_state_no_lock(store: Arc<ECStore>) -> S3Result<SiteReplicationState> {
    match read_config_no_lock(store, SITE_REPLICATION_STATE_PATH).await {
        Ok(data) => parse_site_replication_state(&data),
        Err(StorageError::ConfigNotFound) => Ok(SiteReplicationState::default()),
        Err(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to load site replication state: {err}"),
        )),
    }
}

/// Persist-or-clear under an already-held state object lock. Normalizes the
/// peer map exactly once (the historical persist path normalized twice with
/// two full clones — P2-22).
async fn persist_site_replication_state_no_lock(store: Arc<ECStore>, mut state: SiteReplicationState) -> S3Result<()> {
    state.peers = normalize_peer_map_by_identity(state.peers);
    if state.peers.len() <= 1 && state.pending_rotation.is_none() && state.pending_remove.is_none() {
        match delete_config_no_lock(store, SITE_REPLICATION_STATE_PATH).await {
            Ok(()) | Err(StorageError::ConfigNotFound) => Ok(()),
            Err(err) => Err(S3Error::with_message(S3ErrorCode::InternalError, format!("clear state failed: {err}"))),
        }
    } else {
        let data = serde_json::to_vec(&state)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize state failed: {e}")))?;
        save_config_no_lock(store, SITE_REPLICATION_STATE_PATH, data)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("save state failed: {e}")))
    }
}

/// What a state transaction closure decided to do with the state it was
/// handed. `Unchanged` skips the write entirely: the ack markers and the
/// pending-clearing paths run on every retry and mostly find their pending id
/// already gone, and the retry queue shares this object — rewriting it byte
/// for byte only makes those misses contend with the writers that do have
/// something to say.
enum StateCommit<T> {
    Changed(T),
    Unchanged(T),
}

/// The site-replication state RMW transaction: load, mutate, persist — all
/// under the distributed state-object write lock (see
/// crate::admin::site_replication_state). No peer network calls and no other
/// config locks inside `update`; anything that has to talk to a peer belongs
/// between two transactions, with the precondition re-checked inside the
/// second one.
async fn update_site_replication_state<T, F>(update: F) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut SiteReplicationState) -> S3Result<T> + Send + 'static,
{
    update_site_replication_state_when_changed(move |state| update(state).map(StateCommit::Changed)).await
}

/// [`update_site_replication_state`] for closures that may find nothing to
/// do — see [`StateCommit`].
async fn update_site_replication_state_when_changed<T, F>(update: F) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut SiteReplicationState) -> S3Result<StateCommit<T>> + Send + 'static,
{
    with_site_replication_state_lock(move || async move {
        let store = current_object_store_handle()
            .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?;
        let mut state = load_site_replication_state_no_lock(store.clone()).await?;
        match update(&mut state)? {
            StateCommit::Changed(result) => {
                persist_site_replication_state_no_lock(store, state).await?;
                Ok(result)
            }
            StateCommit::Unchanged(result) => Ok(result),
        }
    })
    .await
}

async fn load_site_replication_repair_state_from_store(store: Arc<ECStore>) -> S3Result<SiteReplicationRepairState> {
    match read_config_no_lock(store, SITE_REPLICATION_REPAIR_STATE_PATH).await {
        Ok(data) => serde_json::from_slice(&data).map_err(|e| {
            S3Error::with_message(S3ErrorCode::InternalError, format!("invalid site replication repair state: {e}"))
        }),
        Err(StorageError::ConfigNotFound) => Ok(SiteReplicationRepairState::default()),
        Err(err) => Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("failed to load site replication repair state: {err}"),
        )),
    }
}

async fn save_site_replication_repair_state_to_store(store: Arc<ECStore>, state: &SiteReplicationRepairState) -> S3Result<()> {
    let data = serde_json::to_vec(state)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize repair state failed: {e}")))?;
    save_config_no_lock(store, SITE_REPLICATION_REPAIR_STATE_PATH, data)
        .await
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("save repair state failed: {e}")))
}

async fn read_site_replication_repair_state() -> S3Result<SiteReplicationRepairState> {
    let store =
        current_object_store_handle().ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?;
    let read_store = store.clone();
    with_config_object_read_lock(store, SITE_REPLICATION_REPAIR_STATE_PATH.to_string(), move || async move {
        load_site_replication_repair_state_from_store(read_store).await
    })
    .await
    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("lock repair state failed: {e}")))?
}

async fn update_site_replication_repair_state<T, F>(update: F) -> S3Result<T>
where
    T: Send + 'static,
    F: FnOnce(&mut SiteReplicationRepairState) -> S3Result<T> + Send + 'static,
{
    let store =
        current_object_store_handle().ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?;
    let read_store = store.clone();
    let save_store = store.clone();
    with_config_object_write_lock(store, SITE_REPLICATION_REPAIR_STATE_PATH.to_string(), move || async move {
        let mut state = load_site_replication_repair_state_from_store(read_store).await?;
        let result = update(&mut state)?;
        save_site_replication_repair_state_to_store(save_store, &state).await?;
        Ok(result)
    })
    .await
    .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("lock repair state failed: {e}")))?
}

/// Test-only seeding of the state object. Every production write goes through
/// [`update_site_replication_state`] — this helper is `cfg(test)` so a new
/// call site cannot reintroduce the pre-P1-15 shape (load through one object
/// lock, save through another, with the mutation in between unprotected).
#[cfg(test)]
async fn save_site_replication_state(state: &SiteReplicationState) -> S3Result<()> {
    let Some(store) = current_object_store_handle() else {
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

fn build_site_replication_peer_client(outbound_tls: &GlobalPublishedOutboundTlsState) -> S3Result<reqwest::Client> {
    build_site_replication_peer_client_with_resolver(outbound_tls, PeerDnsResolver::new(loopback_replication_targets_allowed()))
}

fn build_site_replication_peer_client_with_resolver(
    outbound_tls: &GlobalPublishedOutboundTlsState,
    resolver: PeerDnsResolver,
) -> S3Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(SITE_REPLICATION_PEER_REQUEST_TIMEOUT)
        .connect_timeout(SITE_REPLICATION_PEER_CONNECT_TIMEOUT)
        .pool_idle_timeout(Some(Duration::from_secs(60)))
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(resolver);

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

fn build_custom_site_replication_peer_client(
    outbound_tls: &GlobalPublishedOutboundTlsState,
    connection: &PeerConnection,
) -> S3Result<reqwest::Client> {
    build_custom_site_replication_peer_client_with_resolver(
        outbound_tls,
        connection,
        PeerDnsResolver::new(loopback_replication_targets_allowed()),
    )
}

fn build_custom_site_replication_peer_client_with_resolver(
    outbound_tls: &GlobalPublishedOutboundTlsState,
    connection: &PeerConnection,
    resolver: PeerDnsResolver,
) -> S3Result<reqwest::Client> {
    let mut builder = reqwest::Client::builder()
        .no_proxy()
        .timeout(SITE_REPLICATION_PEER_REQUEST_TIMEOUT)
        .connect_timeout(SITE_REPLICATION_PEER_CONNECT_TIMEOUT)
        .pool_idle_timeout(Some(Duration::from_secs(60)))
        .redirect(reqwest::redirect::Policy::none())
        .dns_resolver(resolver)
        .danger_accept_invalid_certs(connection.skip_tls_verify);

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
    if !connection.ca_cert_pem.is_empty() {
        for cert in parse_peer_ca_certificates(&connection.ca_cert_pem)? {
            builder = builder.add_root_certificate(cert);
        }
    }

    builder
        .build()
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("build site replication peer client failed: {e}")))
}

async fn site_replication_peer_client() -> S3Result<reqwest::Client> {
    let generation = current_outbound_tls_generation().0;
    let cache = SITE_REPLICATION_PEER_CLIENT.lock().await;
    if let Some(hit) = site_replication_peer_client_cache_hit(&cache, generation) {
        return hit;
    }
    drop(cache);

    let outbound_tls = current_outbound_tls_state().await;
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

async fn site_replication_client_for(connection: &PeerConnection) -> S3Result<reqwest::Client> {
    // Revalidate at the client boundary so callers cannot bypass endpoint/TLS policy.
    let connection = PeerConnection::new(connection.endpoint(), connection.skip_tls_verify, &connection.ca_cert_pem)?;
    if connection.uses_default_tls() {
        return site_replication_peer_client().await;
    }
    let outbound_tls = current_outbound_tls_state().await;
    build_custom_site_replication_peer_client(&outbound_tls, &connection)
}

fn runtime_peer_connection(peer: &PeerInfo) -> S3Result<PeerConnection> {
    PeerConnection::try_from(peer).map_err(|err| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("invalid persisted site replication peer `{}`: {err}", peer.endpoint),
        )
    })
}

struct PeerTransport {
    connection: PeerConnection,
    client: reqwest::Client,
}

impl PeerTransport {
    async fn for_runtime_peer(peer: &PeerInfo) -> S3Result<Self> {
        let connection = runtime_peer_connection(peer)?;
        let client = site_replication_client_for(&connection).await.map_err(|err| {
            S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("initialize persisted site replication peer `{}` transport failed: {err}", peer.endpoint),
            )
        })?;
        Ok(Self { connection, client })
    }
}

fn runtime_tls_enabled_with(endpoints: Option<&crate::admin::storage_api::runtime::EndpointServerPools>) -> bool {
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
    let endpoints = current_endpoints_handle();
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
    let Some(config) = current_server_config() else {
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
            current_endpoints_handle().and_then(|endpoints| {
                endpoints
                    .as_ref()
                    .iter()
                    .flat_map(|pool| pool.endpoints.as_ref().iter())
                    .find(|endpoint| endpoint.is_local)
                    .map(|endpoint| endpoint.host_port())
            })
        })
        .unwrap_or_else(|| format!("127.0.0.1:{}", current_runtime_port()));

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
            if parsed.port_or_known_default() == runtime_console_port() && parsed.set_port(Some(current_runtime_port())).is_ok() {
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

fn stored_peer_tls_settings(stored_peer: Option<&PeerInfo>) -> (bool, String) {
    stored_peer
        .map(|peer| (peer.skip_tls_verify, peer.ca_cert_pem.clone()))
        .unwrap_or_default()
}

fn current_local_peer(req: &S3Request<Body>, state: &SiteReplicationState) -> PeerInfo {
    local_peer_at_endpoint(site_replication_local_endpoint(&req.uri, &req.headers), state)
}

/// The local peer record as the given state describes it. Split out of
/// [`current_local_peer`] so a state transaction can rebuild it against the
/// state it just loaded: the request the endpoint came from cannot cross into
/// the transaction closure, but the endpoint itself can.
fn local_peer_at_endpoint(endpoint: String, state: &SiteReplicationState) -> PeerInfo {
    let deployment_id = current_deployment_id().unwrap_or_else(|| deployment_id_for_endpoint(&endpoint));
    let stored_peer = state.peers.get(&deployment_id);
    let (skip_tls_verify, ca_cert_pem) = stored_peer_tls_settings(stored_peer);

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
        skip_tls_verify,
        ca_cert_pem,
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    }
}

fn current_local_runtime_peer(state: &SiteReplicationState) -> PeerInfo {
    local_peer_at_endpoint(current_local_runtime_endpoint(), state)
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

fn existing_peer_for_edit<'a>(state: &'a SiteReplicationState, incoming: &PeerInfo) -> Option<&'a PeerInfo> {
    state.peers.get(&incoming.deployment_id).or_else(|| {
        state
            .peers
            .values()
            .find(|peer| same_identity_endpoint(&peer.endpoint, &incoming.endpoint))
    })
}

fn apply_public_peer_edit_tls_presence(state: &SiteReplicationState, incoming: &mut PeerInfo, presence: PeerTlsFieldPresence) {
    let existing = existing_peer_for_edit(state, incoming);
    let Some(existing) = existing else {
        return;
    };
    if !presence.has_skip_tls_verify() {
        incoming.skip_tls_verify = existing.skip_tls_verify;
    }
    if !presence.has_ca_cert_pem() {
        incoming.ca_cert_pem = existing.ca_cert_pem.clone();
    }
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
        skip_tls_verify: site.skip_tls_verify,
        ca_cert_pem: site.ca_cert_pem,
        api_version: Some(SITE_REPL_API_VERSION.to_string()),
    })
}

fn loopback_replication_targets_allowed() -> bool {
    std::env::var(ALLOW_LOOPBACK_REPLICATION_TARGET_ENV)
        .map(|value| value.eq_ignore_ascii_case("true") || value == "1")
        .unwrap_or(false)
}

fn validate_peer_egress(url: &Url, allow_loopback: bool) -> Result<(), OutboundUrlError> {
    match validate_outbound_url(url) {
        Ok(()) => Ok(()),
        Err(OutboundUrlError::ForbiddenHost {
            reason: "private address",
            ..
        }) => Ok(()),
        Err(OutboundUrlError::ForbiddenHost {
            reason: "loopback address" | "loopback host",
            ..
        }) if allow_loopback && peer_url_has_canonical_loopback_host(url) => Ok(()),
        Err(err) => Err(err),
    }
}

fn peer_url_has_canonical_loopback_host(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(url::Host::Ipv4(ip)) => ip == std::net::Ipv4Addr::LOCALHOST,
        Some(url::Host::Ipv6(ip)) => ip == std::net::Ipv6Addr::LOCALHOST,
        None => false,
    }
}

fn resolved_peer_ip_allowed(host: &str, ip: IpAddr, allow_loopback: bool) -> bool {
    let Ok(ip_url) = (match ip {
        IpAddr::V4(ip) => Url::parse(&format!("http://{ip}")),
        IpAddr::V6(ip) => Url::parse(&format!("http://[{ip}]")),
    }) else {
        return false;
    };
    match validate_outbound_url(&ip_url) {
        Ok(()) => true,
        Err(OutboundUrlError::ForbiddenHost {
            reason: "private address",
            ..
        }) => true,
        Err(OutboundUrlError::ForbiddenHost {
            reason: "loopback address",
            ..
        }) => {
            allow_loopback
                && host.eq_ignore_ascii_case("localhost")
                && matches!(ip, IpAddr::V4(std::net::Ipv4Addr::LOCALHOST) | IpAddr::V6(std::net::Ipv6Addr::LOCALHOST))
        }
        Err(_) => false,
    }
}

fn parse_peer_ca_certificates(ca_cert_pem: &str) -> S3Result<Vec<reqwest::Certificate>> {
    if ca_cert_pem.len() > MAX_PEER_CA_CERT_PEM_SIZE {
        return Err(s3_error!(InvalidRequest, "site replication CA certificate exceeds 256 KiB"));
    }
    if ca_cert_pem.contains("PRIVATE KEY-----") {
        return Err(s3_error!(
            InvalidRequest,
            "site replication CA certificate must not contain a private key"
        ));
    }

    let mut reader = std::io::BufReader::new(ca_cert_pem.as_bytes());
    let certs_der = rustls_pki_types::CertificateDer::pem_reader_iter(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site replication CA certificate: {e}"))
        })?;
    if certs_der.is_empty() {
        return Err(s3_error!(
            InvalidRequest,
            "site replication CA certificate must contain at least one certificate"
        ));
    }

    let mut root_store = rustls::RootCertStore::empty();
    certs_der
        .into_iter()
        .map(|cert| {
            root_store.add(cert.clone()).map_err(|e| {
                S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site replication CA certificate: {e}"))
            })?;
            reqwest::Certificate::from_der(cert.as_ref()).map_err(|e| {
                S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site replication CA certificate: {e}"))
            })
        })
        .collect()
}

fn validate_peer_connection_inner(
    endpoint: &str,
    skip_tls_verify: bool,
    ca_cert_pem: &str,
    allow_loopback: bool,
) -> S3Result<PeerConnection> {
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
    if !parsed.username().is_empty() || parsed.password().is_some() {
        return Err(s3_error!(InvalidRequest, "invalid site endpoint `{endpoint}`: userinfo is not allowed"));
    }
    if parsed.path() != "/" || parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(s3_error!(
            InvalidRequest,
            "invalid site endpoint `{endpoint}`: endpoint must be an origin"
        ));
    }
    validate_peer_egress(&parsed, allow_loopback)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site endpoint `{endpoint}`: {e}")))?;

    if ca_cert_pem.len() > MAX_PEER_CA_CERT_PEM_SIZE {
        return Err(s3_error!(InvalidRequest, "site replication CA certificate exceeds 256 KiB"));
    }
    let ca_cert_pem = ca_cert_pem.trim();
    if parsed.scheme() != "https" && (skip_tls_verify || !ca_cert_pem.is_empty()) {
        return Err(s3_error!(InvalidRequest, "site replication TLS settings require an HTTPS endpoint"));
    }
    if skip_tls_verify && !ca_cert_pem.is_empty() {
        return Err(s3_error!(InvalidRequest, "skipTLSVerify and caCertPem are mutually exclusive"));
    }
    if !ca_cert_pem.is_empty() {
        parse_peer_ca_certificates(ca_cert_pem)?;
    }

    Ok(PeerConnection {
        endpoint: parsed,
        skip_tls_verify,
        ca_cert_pem: ca_cert_pem.to_string(),
    })
}

fn validate_proposed_peer(peer: &PeerInfo) -> S3Result<()> {
    PeerConnection::try_from(peer).map(|_| ())
}

fn validate_join_peer_snapshot(peers: &BTreeMap<String, PeerInfo>) -> S3Result<()> {
    for (deployment_id, peer) in peers {
        validate_proposed_peer(peer).map_err(|err| {
            S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                format!("invalid site replication peer `{deployment_id}`: {err}"),
            )
        })?;
    }
    Ok(())
}

fn peer_tls_is_non_default(skip_tls_verify: bool, ca_cert_pem: &str) -> bool {
    skip_tls_verify || !ca_cert_pem.trim().is_empty()
}

fn add_peer_tls_capability_required(sites: &[PeerSite]) -> bool {
    sites
        .iter()
        .any(|site| peer_tls_is_non_default(site.skip_tls_verify, &site.ca_cert_pem))
}

fn peer_tls_capability_probe_sites(sites: &[PeerSite]) -> Vec<&PeerSite> {
    let mut seen = HashSet::new();
    sites
        .iter()
        .filter(|site| seen.insert(site_identity_key(&site.endpoint)))
        .collect()
}

fn edit_peer_tls_capability_required(existing: Option<&PeerInfo>, proposed: &PeerInfo) -> bool {
    peer_tls_is_non_default(proposed.skip_tls_verify, &proposed.ca_cert_pem)
        && existing.is_none_or(|existing| {
            existing.skip_tls_verify != proposed.skip_tls_verify || existing.ca_cert_pem.trim() != proposed.ca_cert_pem.trim()
        })
}

fn peer_tls_settings_changed(existing: Option<&PeerInfo>, proposed: &PeerInfo) -> bool {
    existing.is_some_and(|existing| {
        existing.skip_tls_verify != proposed.skip_tls_verify || existing.ca_cert_pem.trim() != proposed.ca_cert_pem.trim()
    })
}

fn peer_edit_capability_supported(capability: &str) -> bool {
    matches!(capability, "endpoint-target-refresh" | "peer-tls-settings")
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
        PeerConnection::try_from(site)?;
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

/// The web console's "Set Up Site Replication" flow sends only the remote peer(s) and omits the
/// local deployment from the add payload. The add preflight requires the local deployment to be
/// present (`validate_add_preflight_topology`), so inject the local site when the payload does not
/// already include it. `mc admin replicate add` includes every site (matched here by endpoint
/// identity), so this is a no-op for the CLI. The local site carries no credentials — they are not
/// required for the local peer (`validate_add_sites` skips credential checks for it).
fn ensure_local_site_present(sites: &mut Vec<PeerSite>, local_peer: &PeerInfo) {
    if sites
        .iter()
        .any(|site| same_identity_endpoint(&site.endpoint, &local_peer.endpoint))
    {
        return;
    }
    sites.insert(
        0,
        PeerSite {
            name: local_peer.name.clone(),
            endpoint: local_peer.endpoint.clone(),
            access_key: String::new(),
            secret_key: String::new(),
            skip_tls_verify: local_peer.skip_tls_verify,
            ca_cert_pem: local_peer.ca_cert_pem.clone(),
        },
    );
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
    let bucket_names = info.buckets.keys().cloned().collect();
    Ok(SiteReplicationAddPreflightInfo {
        name: if info.name.is_empty() { site.name.clone() } else { info.name },
        endpoint: site.endpoint.clone(),
        deployment_id: info.deployment_id,
        enabled: info.enabled,
        bucket_count: info.buckets.len(),
        bucket_names,
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
    let connection = PeerConnection::try_from(site)?;
    let client = site_replication_client_for(&connection).await?;
    let info_body = send_peer_admin_get_request_with_client(
        &client,
        &connection,
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

    let idp_body = send_peer_admin_get_request_with_client(
        &client,
        &connection,
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

fn with_site_replication_bootstrap_token(path: &str, token: &str) -> String {
    let separator = if path.contains('?') { '&' } else { '?' };
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair("bootstrapToken", token)
        .finish();
    format!("{path}{separator}{query}")
}

fn site_replication_bootstrap_token(uri: &Uri) -> Option<String> {
    query_pairs(uri).get("bootstrapToken").cloned()
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
    if let Some(local_site) = sites
        .iter()
        .find(|site| same_identity_endpoint(&site.endpoint, &normalized_local.endpoint))
        && (is_https_endpoint(&local_site.endpoint) || !is_https_endpoint(&normalized_local.endpoint))
    {
        normalized_local.endpoint = local_site.endpoint.clone();
        normalized_local.skip_tls_verify = local_site.skip_tls_verify;
        normalized_local.ca_cert_pem = local_site.ca_cert_pem.clone();
    }
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
        peer.skip_tls_verify = site.skip_tls_verify;
        peer.ca_cert_pem = site.ca_cert_pem;
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

fn initialize_join_peer_sync_state(peers: &mut BTreeMap<String, PeerInfo>, defer_sync_state_enable: bool) {
    if !defer_sync_state_enable {
        mark_unknown_peer_sync_enabled(peers);
    }
}

/// Whether an incoming peer join carries a snapshot this site has already
/// moved past — an unstamped join against a configured site, or one whose
/// `updated_at` is not newer. Applying it would roll the local view back to
/// the older topology, so the join is answered as a no-op (MinIO-compatible
/// behaviour, kept verbatim from the pre-transaction handler).
fn join_request_is_superseded(state: &SiteReplicationState, incoming_updated_at: Option<OffsetDateTime>) -> bool {
    let Some(current_updated_at) = state.updated_at else {
        return false;
    };
    incoming_updated_at.is_none_or(|incoming_updated_at| incoming_updated_at <= current_updated_at)
}

/// Adopt an accepted peer join: the sending site's snapshot replaces the local
/// topology wholesale.
///
/// The peer-edit high-water marks are deliberately KEPT. Wiping them here
/// would reopen the exact window the fence closes: every join fan-out (adds
/// AND service-account rotations deliver `SRPeerJoin` to existing peers)
/// would discard live marks, letting a stalled older edit from a peer that
/// never left roll a record back. The one case a kept mark misfences — a
/// site removed while unreachable rejoining with a restarted generation
/// counter — already misfences its ordinary edits identically (pre-existing
/// since the fence landed) and needs an epoch in the fence to fix, not a
/// blanket reset. Marks of origins that left AND were observed leaving are
/// dropped on load by `parse_site_replication_state`.
fn apply_peer_join(
    state: &mut SiteReplicationState,
    local_peer: &PeerInfo,
    join_req: SRPeerJoinReq,
    defer_sync_state_enable: bool,
) {
    state.service_account_access_key = join_req.svc_acct_access_key;
    state.service_account_parent = join_req.svc_acct_parent;
    state.updated_at = join_req.updated_at.or_else(|| Some(OffsetDateTime::now_utc()));
    state.peers = normalize_join_peers_for_local(local_peer, join_req.peers);
    initialize_join_peer_sync_state(&mut state.peers, defer_sync_state_enable);
    state.sync_state_initialized = true;
    state.name = state
        .peers
        .get(&local_peer.deployment_id)
        .map(|peer| peer.name.clone())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| local_peer.name.clone());
}

fn reconcile_peer_with_actual_identity(mut state: SiteReplicationState, actual_peer: PeerInfo) -> SiteReplicationState {
    let mut actual_peer = normalize_peer_info(actual_peer);
    if let Some(requested_peer) = state
        .peers
        .values()
        .find(|peer| same_identity_endpoint(&peer.endpoint, &actual_peer.endpoint))
    {
        actual_peer.skip_tls_verify = requested_peer.skip_tls_verify;
        actual_peer.ca_cert_pem = requested_peer.ca_cert_pem.clone();
    }
    state
        .peers
        .retain(|_, peer| !same_identity_endpoint(&peer.endpoint, &actual_peer.endpoint));
    state.peers.insert(actual_peer.deployment_id.clone(), actual_peer);
    state.peers = normalize_peer_map_by_identity(state.peers);
    state
}

async fn site_replicator_service_account_secret(access_key: &str) -> S3Result<String> {
    let Some(iam_sys) = current_iam_handle() else {
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
    let Some(iam_sys) = current_iam_handle() else {
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
                    parent_user: None,
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
    let Some(iam_sys) = current_iam_handle() else {
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

/// Whether a bucket target is one this site's own peer topology produced.
///
/// Bucket targets are writable by anyone holding `admin:SetBucketTarget`, so a target that
/// merely carries the `site-replicator-0` access key proves nothing: an attacker with only
/// that permission could plant a secret of their choosing and have reconciliation recreate
/// the broadly privileged replication account with it. Require the target to name a peer in
/// the persisted state *and* to point at that peer's recorded endpoint, which is state only
/// `replicate add`/`edit` can write.
fn bucket_target_matches_configured_peer(target: &BucketTarget, state: &SiteReplicationState) -> bool {
    let Some(deployment_id) = bucket_target_deployment_id(target) else {
        return false;
    };
    state
        .peers
        .get(&deployment_id)
        .is_some_and(|peer| bucket_target_endpoint(target) == canonical_endpoint(&peer.endpoint))
}

/// Recover the shared site-replication secret from a bucket target belonging to a configured
/// peer.
///
/// Every site-replication bucket target stores the `site-replicator-0` credentials in
/// `BUCKET_TARGETS_FILE`, which is not encrypted with the root credentials. That makes it the
/// one local copy that survives a root-credential change, so it can reseed the IAM account
/// when the IAM record itself became unreadable.
///
/// Returns `None` unless every matching target agrees on the secret: disagreement means at
/// least one was written by something other than this site's own reconciliation, and picking
/// either would be a guess.
async fn site_replicator_secret_from_bucket_targets(access_key: &str, state: &SiteReplicationState) -> Option<String> {
    let store = current_object_store_handle()?;
    let buckets = store.list_bucket(&BucketOptions::default()).await.ok()?;
    let mut recovered: Option<String> = None;

    for bucket in buckets {
        let Ok(targets) = metadata_sys::list_bucket_targets(&bucket.name).await else {
            continue;
        };
        for target in targets.targets {
            if target.target_type != BucketTargetType::ReplicationService
                || !bucket_target_matches_configured_peer(&target, state)
            {
                continue;
            }
            let Some(credentials) = target.credentials.as_ref() else {
                continue;
            };
            if credentials.access_key != access_key || credentials.secret_key.is_empty() {
                continue;
            }
            match &recovered {
                Some(seen) if seen != &credentials.secret_key => return None,
                Some(_) => {}
                None => recovered = Some(credentials.secret_key.clone()),
            }
        }
    }

    recovered
}

/// Whether the parent recorded in site-replication state can actually back a service account.
///
/// Repairing against a parent that does not exist would produce an account no policy path can
/// resolve, so a missing parent means "leave the current binding alone and report it".
async fn site_replicator_parent_is_usable(parent: &str) -> bool {
    if rustfs_iam::is_root_access_key(parent) {
        return true;
    }
    match current_iam_handle() {
        Some(iam_sys) => iam_sys.get_user_info(parent).await.is_ok(),
        None => false,
    }
}

/// Whether an IAM lookup failure means the account is absent or unreadable, as opposed to a
/// transient store failure. Only the former may trigger a reseed from bucket targets.
fn is_missing_service_account_error(err: &rustfs_iam::error::Error) -> bool {
    matches!(
        err,
        rustfs_iam::error::Error::NoSuchAccount(_)
            | rustfs_iam::error::Error::NoSuchServiceAccount(_)
            | rustfs_iam::error::Error::NoSuchUser(_)
            | rustfs_iam::error::Error::ConfigNotFound
    )
}

/// Reconcile the local `site-replicator-0` account against the persisted site-replication
/// state, repairing the two drifts that no other code path can undo.
///
/// `update_service_account` cannot rewrite `parent_user`, so once the account is bound to a
/// parent that a root-credential change invalidated, every later `replicate add` takes the
/// update branch and preserves the stale binding forever. Worse, IAM records encrypted with
/// the previous root secret fail to decrypt and surface as "no such account", which silently
/// disables every control-plane push while `replicate info` still reports the site enabled.
/// Both used to require deleting and recreating the account by hand.
async fn reconcile_site_replicator_service_account() -> S3Result<()> {
    // Read-only against the state: `load_site_replication_state` takes the
    // object read lock on its own, and everything after it is IAM work.
    let state = load_site_replication_state().await?;
    if !state.enabled() || state.service_account_access_key != SITE_REPLICATOR_SERVICE_ACCOUNT {
        return Ok(());
    }

    let Some(iam_sys) = current_iam_handle() else {
        return Err(s3_error!(InvalidRequest, "iam not init"));
    };

    let parent_user = state.service_account_parent.clone();
    if parent_user.is_empty() {
        warn!(
            event = EVENT_ADMIN_SITE_REPLICATION_STATE,
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
            result = "service_account_parent_unknown",
            "admin site replication state"
        );
        return Ok(());
    }

    let access_key = SITE_REPLICATOR_SERVICE_ACCOUNT;
    let session_policy = site_replicator_service_account_policy()?;
    let reason = match iam_sys.get_site_replicator_service_account_secret(access_key).await {
        Ok(_) => {
            // Never rebuild on an unread parent: `unwrap_or_default` here would compare an
            // empty string against a real parent and repair a healthy account on every boot.
            let Ok((credentials, _)) = iam_sys.get_service_account(access_key).await else {
                return Ok(());
            };
            if credentials.parent_user == parent_user {
                return Ok(());
            }

            if !site_replicator_parent_is_usable(&parent_user).await {
                warn!(
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    result = "service_account_parent_missing",
                    reason = "stale_parent",
                    parent = %parent_user,
                    "admin site replication state"
                );
                return Ok(());
            }

            // The record is intact and may be authenticating replication traffic right now,
            // so rebind it in place. Deleting first would open a window — however brief —
            // where a crash or a storage error leaves the site with no replication account
            // at all, which is worse than the stale binding being repaired.
            iam_sys
                .update_service_account(
                    access_key,
                    UpdateServiceAccountOpts {
                        session_policy: Some(session_policy),
                        secret_key: None,
                        name: None,
                        description: None,
                        expiration: None,
                        status: None,
                        parent_user: Some(parent_user.clone()),
                        allow_site_replicator_account: true,
                    },
                )
                .await
                .map_err(ApiError::from)?;
            "stale_parent"
        }
        // Only a genuinely absent or unreadable account may be reseeded from a bucket
        // target. A transient store error must not trigger a rewrite of a live account.
        Err(err) if is_missing_service_account_error(&err) => {
            let Some(secret) = site_replicator_secret_from_bucket_targets(access_key, &state).await else {
                warn!(
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    result = "service_account_unrecoverable",
                    error = ?err,
                    "admin site replication state"
                );
                return Ok(());
            };

            if !site_replicator_parent_is_usable(&parent_user).await {
                warn!(
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    result = "service_account_parent_missing",
                    reason = "account_unreadable",
                    parent = %parent_user,
                    "admin site replication state"
                );
                return Ok(());
            }

            // Nothing readable to preserve, so creation is the whole repair — there is no
            // delete to leave a gap behind.
            iam_sys
                .new_service_account(
                    &parent_user,
                    None,
                    NewServiceAccountOpts {
                        session_policy: Some(session_policy),
                        access_key: access_key.to_string(),
                        secret_key: secret,
                        name: None,
                        description: None,
                        expiration: None,
                        allow_site_replicator_account: true,
                        claims: None,
                    },
                )
                .await
                .map_err(ApiError::from)?;
            "account_unreadable"
        }
        Err(err) => return Err(ApiError::from(err).into()),
    };

    warn!(
        event = EVENT_ADMIN_SITE_REPLICATION_STATE,
        component = LOG_COMPONENT_ADMIN,
        subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
        result = "service_account_repaired",
        reason,
        parent = %parent_user,
        "admin site replication state"
    );

    Ok(())
}

/// Rebuild every replicated bucket's outbound rules and targets from the current peer set.
///
/// A bucket whose `site-repl-*` rule was overwritten by a peer's config points at this very
/// deployment and replicates nothing. Nothing else revisits an existing bucket — the rule
/// builders only run on bucket creation, peer bucket-ops and metadata pushes — so without a
/// pass here an upgraded site keeps the broken rules until someone recreates the bucket.
/// Reconciliation is a no-op write-wise when the rules already match.
async fn reconcile_site_replication_buckets() -> S3Result<()> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(());
    };
    let Some(store) = current_object_store_handle() else {
        return Ok(());
    };
    let buckets = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;

    for bucket in buckets {
        if let Err(err) = ensure_site_replication_bucket_setup_with_runtime(&bucket.name, &runtime).await {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                bucket = %bucket.name,
                result = "bucket_reconcile_failed",
                error = ?err,
                "admin site replication state"
            );
            continue;
        }

        // Once per bucket per pass, so an operator sees a rule that resolves to nothing
        // without the replication hot path logging it for every object. Reconciliation
        // cannot fix this case: the rules are right and the peer endpoint is not reachable.
        if let Ok(metadata) = metadata_sys::get(&bucket.name).await
            && !site_replication_targets_online(&bucket.name, &metadata.replication_config_xml).await
        {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                bucket = %bucket.name,
                result = "replication_target_offline",
                "a site replication rule has no usable remote target; objects for it are not replicating"
            );
        }
    }

    Ok(())
}

/// Repair drifted site-replication wiring: the service account first, since the bucket pass
/// signs its targets with that account's secret.
///
/// Registered into the infra-layer scheduler (`site_replication_reconcile`) rather than
/// called from it, so startup never has to reach up into this layer.
///
/// Gives up the whole round rather than racing a multi-phase operation. Two mechanisms are
/// needed: the lifecycle lock covers add and remove, while an endpoint refresh commits
/// bucket targets and peer state in separate steps *without* holding that lock
/// (`SiteReplicationEditHandler`), so a tick landing between them would rewrite the targets
/// from the stale endpoint. The pending marker in the persisted state closes that window.
/// Skipping costs nothing — the timer comes back.
fn reconcile_site_replication_wiring() -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> {
    Box::pin(async {
        // The scheduler starts before IAM and the object store are guaranteed ready (IAM
        // bootstrap may still be recovering), so an early tick returns quietly instead of
        // logging a failure for every reconciler.
        if current_iam_handle().is_none() || current_object_store_handle().is_none() {
            return;
        }

        let Some(_lifecycle) = SiteReplicationLifecycleGuard::try_acquire() else {
            return;
        };

        match load_site_replication_state().await {
            Ok(state) => {
                if state.pending_endpoint_refresh.is_some() || state.pending_remove.is_some() || state.pending_rotation.is_some()
                {
                    return;
                }
            }
            // Unreadable state is reported by the reconcilers below; do not double-log here.
            Err(_) => return,
        }

        if let Err(err) = reconcile_site_replicator_service_account().await {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                result = "service_account_reconcile_failed",
                error = ?err,
                "admin site replication state"
            );
        }
        if let Err(err) = reconcile_site_replication_buckets().await {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                result = "bucket_reconcile_failed",
                error = ?err,
                "admin site replication state"
            );
        }
    })
}

fn site_replication_peer_wire_path(path: &str) -> String {
    let (path_only, query) = path
        .split_once('?')
        .map(|(path, query)| (path, Some(query)))
        .unwrap_or((path, None));
    let wire_path = if let Some(suffix) = path_only.strip_prefix(RUSTFS_ADMIN_V3_PREFIX) {
        format!("{MINIO_ADMIN_V3_PREFIX}{suffix}")
    } else {
        path_only.to_string()
    };

    match query {
        Some(query) => format!("{wire_path}?{query}"),
        None => wire_path,
    }
}

fn site_replication_peer_payload_encrypted(wire_path: &str) -> bool {
    // MinIO's SRPeerJoin handler force-decrypts the request body, so the
    // peer/join payload must always travel encrypted.
    wire_path.split_once('?').map(|(path, _)| path).unwrap_or(wire_path) == MINIO_SITE_REPLICATION_PEER_JOIN_PATH
}

fn site_replication_peer_payload(path: &str, secret_key: &str, payload: Vec<u8>) -> S3Result<(Vec<u8>, &'static str)> {
    if site_replication_peer_payload_encrypted(path) {
        encode_compatible_admin_payload(path, secret_key, payload)
    } else {
        Ok((payload, "application/json"))
    }
}

fn site_replication_peer_url(connection: &PeerConnection, wire_path: &str) -> S3Result<Url> {
    let path = wire_path.split_once('?').map_or(wire_path, |(path, _)| path);
    if !path.starts_with('/') || path.starts_with("//") {
        return Err(s3_error!(InvalidRequest, "invalid site replication peer path"));
    }
    connection
        .endpoint
        .join(wire_path)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InvalidRequest, format!("invalid site replication peer path: {e}")))
}

#[cfg(test)]
async fn send_peer_admin_request_raw<T: Serialize>(
    connection: &PeerConnection,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<(StatusCode, Vec<u8>)> {
    let client = site_replication_client_for(connection).await?;
    send_peer_admin_request_raw_with_client(&client, connection, path, access_key, secret_key, body).await
}

async fn send_peer_admin_request_raw_with_client<T: Serialize>(
    client: &reqwest::Client,
    connection: &PeerConnection,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<(StatusCode, Vec<u8>)> {
    let path = site_replication_peer_wire_path(path);
    let url = site_replication_peer_url(connection, &path)?;
    let uri = url
        .as_str()
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
        current_region()
            .map(|region| region.to_string())
            .as_deref()
            .unwrap_or("us-east-1"),
    );

    let mut req = client.request(reqwest::Method::PUT, url.clone());
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

    Ok((status, body.to_vec()))
}

async fn send_peer_admin_request<T: Serialize>(
    connection: &PeerConnection,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<Vec<u8>> {
    let client = site_replication_client_for(connection).await?;
    send_peer_admin_request_with_client(&client, connection, path, access_key, secret_key, body).await
}

async fn send_peer_admin_request_with_client<T: Serialize>(
    client: &reqwest::Client,
    connection: &PeerConnection,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<Vec<u8>> {
    let (status, body) = send_peer_admin_request_raw_with_client(client, connection, path, access_key, secret_key, body).await?;
    if status.is_success() {
        return Ok(body);
    }

    let detail = String::from_utf8_lossy(&body).into_owned();
    Err(S3Error::with_message(
        S3ErrorCode::InternalError,
        format!("peer request to {}{path} failed with {status}: {detail}", connection.endpoint()),
    ))
}

async fn send_peer_admin_request_with_secret_candidates<T: Serialize>(
    connection: &PeerConnection,
    path: &str,
    access_key: &str,
    secret_candidates: &[String],
    body: &T,
) -> S3Result<Vec<u8>> {
    let client = site_replication_client_for(connection).await?;
    let mut tried = HashSet::new();
    let mut errors = Vec::new();

    for secret_key in secret_candidates.iter().filter(|secret_key| !secret_key.is_empty()) {
        if !tried.insert(secret_key.as_str()) {
            continue;
        }

        match send_peer_admin_request_with_client(&client, connection, path, access_key, secret_key, body).await {
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
            "peer request to {}{path} failed with all service-account secrets: {}",
            connection.endpoint(),
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

async fn send_peer_admin_get_request(
    connection: &PeerConnection,
    path: &str,
    access_key: &str,
    secret_key: &str,
) -> S3Result<Vec<u8>> {
    let client = site_replication_client_for(connection).await?;
    send_peer_admin_get_request_with_client(&client, connection, path, access_key, secret_key).await
}

async fn send_peer_admin_get_request_with_client(
    client: &reqwest::Client,
    connection: &PeerConnection,
    path: &str,
    access_key: &str,
    secret_key: &str,
) -> S3Result<Vec<u8>> {
    let path = site_replication_peer_wire_path(path);
    let url = site_replication_peer_url(connection, &path)?;
    let uri = url
        .as_str()
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
        current_region()
            .map(|region| region.to_string())
            .as_deref()
            .unwrap_or("us-east-1"),
    );

    let mut req = client.request(reqwest::Method::GET, url.clone());
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
    let transport = PeerTransport::for_runtime_peer(peer).await?;
    send_peer_admin_request_with_retry_event_transport(peer, &transport, path, access_key, secret_key, body).await
}

async fn send_peer_admin_request_with_retry_event_transport<T: Serialize>(
    peer: &PeerInfo,
    transport: &PeerTransport,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<Vec<u8>> {
    match send_peer_admin_request_with_client(&transport.client, &transport.connection, path, access_key, secret_key, body).await
    {
        Ok(body) => {
            dequeue_site_replication_retry_event(peer, path).await;
            Ok(body)
        }
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
    let transport = PeerTransport::for_runtime_peer(peer).await?;
    for item in &plan.iam_items {
        send_peer_admin_request_with_retry_event_transport(
            peer,
            &transport,
            "/rustfs/admin/v3/site-replication/peer/iam-item",
            service_account_access_key,
            service_account_secret_key,
            item,
        )
        .await?;
    }

    let empty = serde_json::json!({});
    for path in &plan.bucket_make_ops {
        send_peer_admin_request_with_retry_event_transport(
            peer,
            &transport,
            path,
            service_account_access_key,
            service_account_secret_key,
            &empty,
        )
        .await?;
    }

    for item in &plan.bucket_items {
        send_peer_admin_request_with_retry_event_transport(
            peer,
            &transport,
            "/rustfs/admin/v3/site-replication/peer/bucket-meta",
            service_account_access_key,
            service_account_secret_key,
            item,
        )
        .await?;
    }

    for path in &plan.bucket_configure_ops {
        send_peer_admin_request_with_retry_event_transport(
            peer,
            &transport,
            path,
            service_account_access_key,
            service_account_secret_key,
            &empty,
        )
        .await?;
    }

    Ok(())
}

async fn bootstrap_existing_metadata_after_add(
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    service_account_secret_key: &str,
) -> SiteReplicationErrorSummary {
    let info = match build_sr_info(state, local_peer).await {
        Ok(info) => info,
        Err(err) => {
            let mut errors = SiteReplicationErrorSummary::default();
            errors.push(format!("local snapshot failed: {err}"));
            return errors;
        }
    };
    let plan = match site_replication_bootstrap_plan(&info) {
        Ok(plan) => plan,
        Err(err) => {
            let mut errors = SiteReplicationErrorSummary::default();
            errors.push(format!("bootstrap plan failed: {err}"));
            return errors;
        }
    };

    let mut errors = SiteReplicationErrorSummary::default();
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

enum SiteReplicationRepairTask<'a> {
    Iam(&'a SRIAMItem),
    BucketMake(&'a str),
    BucketMetadata(&'a SRBucketMeta),
    Replication(&'a str),
}

impl SiteReplicationRepairTask<'_> {
    fn family(&self) -> &'static str {
        match self {
            Self::Iam(_) => SITE_REPLICATION_REPAIR_IAM_FAMILY,
            Self::BucketMake(_) => SITE_REPLICATION_REPAIR_BUCKET_FAMILY,
            Self::BucketMetadata(_) => SITE_REPLICATION_REPAIR_BUCKET_METADATA_FAMILY,
            Self::Replication(_) => SITE_REPLICATION_REPAIR_REPLICATION_FAMILY,
        }
    }

    fn path(&self) -> &str {
        match self {
            Self::Iam(_) => "/rustfs/admin/v3/site-replication/peer/iam-item",
            Self::BucketMake(path) | Self::Replication(path) => path,
            Self::BucketMetadata(_) => "/rustfs/admin/v3/site-replication/peer/bucket-meta",
        }
    }

    fn id(&self) -> S3Result<String> {
        let payload = match self {
            Self::Iam(item) => serde_json::to_vec(item),
            Self::BucketMake(_) | Self::Replication(_) => serde_json::to_vec(&serde_json::json!({})),
            Self::BucketMetadata(item) => serde_json::to_vec(item),
        }
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize repair task failed: {err}")))?;
        let mut digest = Sha256::new();
        digest.update(self.family().as_bytes());
        digest.update([0]);
        digest.update(self.path().as_bytes());
        digest.update([0]);
        digest.update(payload);
        Ok(URL_SAFE_NO_PAD.encode(digest.finalize()))
    }

    async fn send(&self, transport: &PeerTransport, access_key: &str, secret_key: &str) -> S3Result<Vec<u8>> {
        match self {
            Self::Iam(item) => {
                send_peer_admin_request_with_client(
                    &transport.client,
                    &transport.connection,
                    self.path(),
                    access_key,
                    secret_key,
                    item,
                )
                .await
            }
            Self::BucketMetadata(item) => {
                send_peer_admin_request_with_client(
                    &transport.client,
                    &transport.connection,
                    self.path(),
                    access_key,
                    secret_key,
                    item,
                )
                .await
            }
            Self::BucketMake(_) | Self::Replication(_) => {
                send_peer_admin_request_with_client(
                    &transport.client,
                    &transport.connection,
                    self.path(),
                    access_key,
                    secret_key,
                    &serde_json::json!({}),
                )
                .await
            }
        }
    }
}

fn site_replication_repair_tasks(plan: &SiteReplicationBootstrapPlan) -> Vec<(usize, SiteReplicationRepairTask<'_>)> {
    let mut tasks = Vec::with_capacity(
        plan.iam_items.len() + plan.bucket_make_ops.len() + plan.bucket_items.len() + plan.bucket_configure_ops.len(),
    );
    tasks.extend(
        plan.iam_items
            .iter()
            .enumerate()
            .map(|(index, item)| (index, SiteReplicationRepairTask::Iam(item))),
    );
    tasks.extend(
        plan.bucket_make_ops
            .iter()
            .enumerate()
            .map(|(index, path)| (index, SiteReplicationRepairTask::BucketMake(path))),
    );
    tasks.extend(
        plan.bucket_items
            .iter()
            .enumerate()
            .map(|(index, item)| (index, SiteReplicationRepairTask::BucketMetadata(item))),
    );
    tasks.extend(
        plan.bucket_configure_ops
            .iter()
            .enumerate()
            .map(|(index, path)| (index, SiteReplicationRepairTask::Replication(path))),
    );
    tasks
}

fn site_replication_repair_plan_token(state: &SiteReplicationState, plan: &SiteReplicationBootstrapPlan) -> S3Result<String> {
    let mut digest = Sha256::new();
    let snapshot = serde_json::to_vec(&(
        &state.name,
        &state.service_account_access_key,
        &state.peers,
        state.updated_at,
        state.sync_state_initialized,
    ))
    .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize repair snapshot failed: {err}")))?;
    digest.update(snapshot);
    for (_, task) in site_replication_repair_tasks(plan) {
        digest.update(task.id()?.as_bytes());
    }
    Ok(URL_SAFE_NO_PAD.encode(digest.finalize()))
}

fn site_replication_repair_preflight_token(
    state: &SiteReplicationState,
    plan: &SiteReplicationBootstrapPlan,
    signing_key: &[u8],
) -> S3Result<String> {
    if signing_key.is_empty() {
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            "repair signing key is empty".to_string(),
        ));
    }
    let mut digest = <Hmac<Sha256> as hmac::digest::KeyInit>::new_from_slice(signing_key)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "invalid repair signing key".to_string()))?;
    digest.update(b"rustfs:site-replication:repair-preflight:v1\0");
    digest.update(site_replication_repair_plan_token(state, plan)?.as_bytes());
    for event in state
        .retry_queue
        .iter()
        .filter(|event| retry_event_replayed_by_bootstrap(event))
    {
        digest.update(event.id.as_bytes());
        digest.update(&[0]);
        digest.update(event.peer_deployment_id.as_bytes());
        digest.update(&[0]);
        digest.update(event.path.as_bytes());
        digest.update(&[0]);
    }
    Ok(URL_SAFE_NO_PAD.encode(digest.finalize().into_bytes()))
}

fn site_replication_repair_task_checkpoint_id(
    signing_key: &[u8],
    peer_deployment_id: &str,
    task: &SiteReplicationRepairTask<'_>,
) -> S3Result<String> {
    let mut digest = <Hmac<Sha256> as hmac::digest::KeyInit>::new_from_slice(signing_key)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "invalid repair signing key".to_string()))?;
    digest.update(b"rustfs:site-replication:repair-task:v1\0");
    digest.update(peer_deployment_id.as_bytes());
    digest.update(&[0]);
    digest.update(task.id()?.as_bytes());
    Ok(URL_SAFE_NO_PAD.encode(digest.finalize().into_bytes()))
}

fn site_replication_repair_sites(
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    plan: &SiteReplicationBootstrapPlan,
    signing_key: &[u8],
) -> S3Result<BTreeMap<String, SiteReplicationRepairSiteStatus>> {
    let mut planned = BTreeMap::new();
    let mut family_paths = BTreeMap::<String, BTreeSet<String>>::new();
    for (_, task) in site_replication_repair_tasks(plan) {
        let family = task.family().to_string();
        let family_status = planned
            .entry(task.family().to_string())
            .or_insert_with(SiteReplicationRepairFamilyStatus::default);
        family_status.planned += 1;
        family_paths.entry(family).or_default().insert(task.path().to_string());
    }

    let mut sites = BTreeMap::new();
    for peer in state.peers.values().filter(|peer| {
        peer.deployment_id != local_peer.deployment_id && !same_identity_endpoint(&peer.endpoint, &local_peer.endpoint)
    }) {
        let mut families = planned.clone();
        for (_, task) in site_replication_repair_tasks(plan) {
            let family = families
                .get_mut(task.family())
                .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair task family is missing".to_string()))?;
            family.tasks.push(SiteReplicationRepairTaskStatus {
                task_id: site_replication_repair_task_checkpoint_id(signing_key, &peer.deployment_id, &task)?,
                status: "planned".to_string(),
                error: None,
            });
        }
        for (family, status) in &mut families {
            status.retry_events = state
                .retry_queue
                .iter()
                .filter(|event| {
                    event.peer_deployment_id == peer.deployment_id
                        && retry_event_replayed_by_bootstrap(event)
                        && family_paths.get(family).is_some_and(|paths| paths.contains(&event.path))
                })
                .count();
        }
        sites.insert(
            peer.deployment_id.clone(),
            SiteReplicationRepairSiteStatus {
                deployment_id: peer.deployment_id.clone(),
                name: peer.name.clone(),
                families,
            },
        );
    }
    Ok(sites)
}

fn update_site_replication_repair_task(
    operation: &mut SiteReplicationRepairOperation,
    deployment_id: &str,
    family: &str,
    family_index: usize,
    result: Result<(), &str>,
) -> S3Result<()> {
    let site = operation
        .sites
        .get_mut(deployment_id)
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair operation site is missing".to_string()))?;
    let family_status = site
        .families
        .get_mut(family)
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair operation family is missing".to_string()))?;
    if family_status.succeeded != family_index {
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            "repair operation task checkpoint is invalid".to_string(),
        ));
    }
    let task_status = family_status.tasks.get_mut(family_index).ok_or_else(|| {
        S3Error::with_message(S3ErrorCode::InternalError, "repair operation task checkpoint is missing".to_string())
    })?;
    family_status.failed = 0;
    family_status.errors.clear();
    match result {
        Ok(()) => {
            family_status.succeeded = family_status.succeeded.saturating_add(1);
            task_status.status = "succeeded".to_string();
            task_status.error = None;
        }
        Err(error) => {
            let error = classify_site_replication_repair_error(error).to_string();
            family_status.failed = 1;
            family_status.errors.push(error.clone());
            task_status.status = "failed".to_string();
            task_status.error = Some(error);
        }
    }
    Ok(())
}

fn site_replication_repair_task_pending(
    operation: &SiteReplicationRepairOperation,
    deployment_id: &str,
    family: &str,
    family_index: usize,
) -> S3Result<bool> {
    let site = operation
        .sites
        .get(deployment_id)
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair operation site is missing".to_string()))?;
    let family = site
        .families
        .get(family)
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair operation family is missing".to_string()))?;
    if family.succeeded > family_index {
        return Ok(false);
    }
    if family.succeeded < family_index {
        return Ok(false);
    }
    Ok(family.failed == 0)
}

fn prepare_site_replication_repair_retry(operation: &mut SiteReplicationRepairOperation) {
    for family in operation.sites.values_mut().flat_map(|site| site.families.values_mut()) {
        family.failed = 0;
        family.errors.clear();
        for task in &mut family.tasks {
            match task.status.as_str() {
                "succeeded" => task.status = "skipped".to_string(),
                "failed" => {
                    task.status = "planned".to_string();
                    task.error = None;
                }
                _ => {}
            }
        }
    }
}

fn classify_site_replication_repair_error(error: &str) -> &'static str {
    let error = error.to_ascii_lowercase();
    if error.contains("accessdenied")
        || error.contains("signaturedoesnotmatch")
        || error.contains("unauthorized")
        || error.contains("forbidden")
        || error.contains("401")
        || error.contains("403")
    {
        "authorization-failed"
    } else if error.contains("timeout") {
        "remote-timeout"
    } else if error.contains("dns") {
        "remote-dns-failed"
    } else if error.contains("tls") || error.contains("certificate") {
        "remote-tls-failed"
    } else if error.contains("connect") {
        "remote-connect-failed"
    } else {
        "remote-operation-failed"
    }
}

fn summarize_site_replication_repair_operation(operation: &mut SiteReplicationRepairOperation) {
    let failed = operation
        .sites
        .values()
        .flat_map(|site| site.families.values())
        .any(|family| family.failed > 0);
    let complete = operation
        .sites
        .values()
        .all(|site| site.families.values().all(|family| family.succeeded == family.planned));
    operation.status = if complete {
        "success"
    } else if failed {
        "partial"
    } else {
        "running"
    }
    .to_string();
    operation.updated_at = Some(OffsetDateTime::now_utc());
    operation.completed_at = complete.then_some(OffsetDateTime::now_utc());
}

fn site_replication_repair_operation_response(
    operation: &SiteReplicationRepairOperation,
) -> SiteReplicationRepairOperationResponse {
    SiteReplicationRepairOperationResponse {
        mode: "execute",
        operation_id: operation.operation_id.clone(),
        status: operation.status.clone(),
        sites: operation
            .sites
            .iter()
            .map(|(deployment_id, site)| {
                (
                    deployment_id.clone(),
                    SiteReplicationRepairSiteResponse {
                        deployment_id: site.deployment_id.clone(),
                        name: site.name.clone(),
                        families: site
                            .families
                            .iter()
                            .map(|(family, status)| {
                                (
                                    family.clone(),
                                    SiteReplicationRepairFamilyResponse {
                                        planned: status.planned,
                                        succeeded: status.succeeded,
                                        failed: status.failed,
                                        retry_events: status.retry_events,
                                        tasks: status.tasks.clone(),
                                        errors: status.errors.clone(),
                                    },
                                )
                            })
                            .collect(),
                    },
                )
            })
            .collect(),
        created_at: operation.created_at,
        updated_at: operation.updated_at,
        completed_at: operation.completed_at,
    }
}

fn prune_site_replication_repair_operations(operations: &mut BTreeMap<String, SiteReplicationRepairOperation>) {
    while operations.len() > SITE_REPLICATION_REPAIR_OPERATION_LIMIT {
        let Some(oldest) = operations
            .iter()
            .filter(|(_, operation)| operation.status == "success")
            .min_by_key(|(_, operation)| operation.created_at)
            .map(|(id, _)| id.clone())
        else {
            break;
        };
        operations.remove(&oldest);
    }
}

async fn persist_site_replication_repair_operation(operation: &SiteReplicationRepairOperation) -> S3Result<()> {
    let operation = operation.clone();
    update_site_replication_repair_state(move |state| {
        if let Some(existing) = state.operations.get(&operation.operation_id)
            && !constant_time_eq(&existing.preflight_token, &operation.preflight_token)
        {
            return Err(S3Error::with_message(
                S3ErrorCode::ClientTokenConflict,
                "repair operation ID is already bound to a different preflight".to_string(),
            ));
        }
        state.operations.insert(operation.operation_id.clone(), operation);
        prune_site_replication_repair_operations(&mut state.operations);
        Ok(())
    })
    .await
}

async fn persist_site_replication_repair_task(
    operation: &SiteReplicationRepairOperation,
    peer: &PeerInfo,
    family: &str,
    path: &str,
) -> S3Result<()> {
    persist_site_replication_repair_operation(operation).await?;

    let family_status = operation
        .sites
        .get(&peer.deployment_id)
        .and_then(|site| site.families.get(family))
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "repair task status is missing".to_string()))?;
    let failure = (family_status.failed > 0).then(|| {
        family_status
            .errors
            .first()
            .cloned()
            .unwrap_or_else(|| "remote-operation-failed".to_string())
    });
    let peer = peer.clone();
    let path = path.to_string();
    update_site_replication_state(move |state| {
        match failure.as_deref() {
            Some(error) => upsert_site_replication_retry_event(&mut state.retry_queue, &peer, &path, error, None),
            None => {
                dequeue_site_replication_retry_events(&mut state.retry_queue, &peer, &path);
            }
        }
        Ok(())
    })
    .await
}

fn admit_site_replication_repair_operation(
    repair_state: &mut SiteReplicationRepairState,
    operation_id: String,
    supplied_token: &str,
    candidate: SiteReplicationRepairOperation,
) -> S3Result<SiteReplicationRepairOperation> {
    if let Some(existing) = repair_state.operations.get(&operation_id) {
        if !constant_time_eq(&existing.preflight_token, supplied_token) {
            return Err(S3Error::with_message(
                S3ErrorCode::ClientTokenConflict,
                "repair operation ID is already bound to a different preflight".to_string(),
            ));
        }
        if !constant_time_eq(&existing.plan_token, &candidate.plan_token) {
            return Err(S3Error::with_message(
                S3ErrorCode::PreconditionFailed,
                "site replication repair plan changed after partial execution".to_string(),
            ));
        }
        return Ok(existing.clone());
    }
    if repair_state
        .operations
        .values()
        .any(|operation| operation.status == "running")
    {
        return Err(S3Error::with_message(
            S3ErrorCode::ClientTokenConflict,
            "another site replication repair is active".to_string(),
        ));
    }
    repair_state.operations.insert(operation_id, candidate.clone());
    prune_site_replication_repair_operations(&mut repair_state.operations);
    Ok(candidate)
}

async fn execute_site_replication_repair(
    request: SiteReplicationRepairExecutionRequest,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let store =
        current_object_store_handle().ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()))?;
    with_config_object_write_lock(store, SITE_REPLICATION_REPAIR_EXECUTION_LOCK_PATH.to_string(), move || async move {
        execute_site_replication_repair_locked(request).await
    })
    .await
    .map_err(|_| {
        S3Error::with_message(S3ErrorCode::ClientTokenConflict, "another site replication repair is active".to_string())
    })?
}

async fn execute_site_replication_repair_locked(
    request: SiteReplicationRepairExecutionRequest,
) -> S3Result<S3Response<(StatusCode, Body)>> {
    let state = load_site_replication_state().await?;
    if !state.enabled() || state.service_account_access_key.is_empty() {
        return Err(s3_error!(InvalidRequest, "site replication is not configured"));
    }
    let info = build_sr_info(&state, &request.local_peer).await?;
    let plan = site_replication_bootstrap_plan(&info)?;
    let plan_token = site_replication_repair_plan_token(&state, &plan)?;
    let preflight_token = site_replication_repair_preflight_token(&state, &plan, request.signing_key.as_bytes())?;
    let sites = site_replication_repair_sites(&state, &request.local_peer, &plan, request.signing_key.as_bytes())?;

    let repair_state = read_site_replication_repair_state().await?;
    if let Some(existing) = repair_state.operations.get(&request.operation_id) {
        if !constant_time_eq(&existing.preflight_token, &request.preflight_token) {
            return Err(S3Error::with_message(
                S3ErrorCode::ClientTokenConflict,
                "repair operation ID is already bound to a different preflight".to_string(),
            ));
        }
        if existing.status == "success" {
            return json_response(&site_replication_repair_operation_response(existing));
        }
        if !constant_time_eq(&existing.plan_token, &plan_token) {
            return Err(S3Error::with_message(
                S3ErrorCode::PreconditionFailed,
                "site replication repair plan changed after partial execution".to_string(),
            ));
        }
    } else if !constant_time_eq(&request.preflight_token, &preflight_token) {
        return Err(S3Error::with_message(
            S3ErrorCode::PreconditionFailed,
            "site replication repair preflight is stale".to_string(),
        ));
    }

    let now = OffsetDateTime::now_utc();
    let candidate = SiteReplicationRepairOperation {
        operation_id: request.operation_id.clone(),
        preflight_token,
        plan_token,
        status: "running".to_string(),
        sites,
        created_at: Some(now),
        updated_at: Some(now),
        completed_at: None,
    };
    let supplied_token = request.preflight_token;
    let operation_id = request.operation_id;
    let mut operation = update_site_replication_repair_state(move |repair_state| {
        admit_site_replication_repair_operation(repair_state, operation_id, &supplied_token, candidate)
    })
    .await?;
    if operation.status == "success" {
        return json_response(&site_replication_repair_operation_response(&operation));
    }

    let service_account_secret_key = site_replicator_service_account_secret(&state.service_account_access_key).await?;
    prepare_site_replication_repair_retry(&mut operation);
    operation.status = "running".to_string();
    operation.completed_at = None;
    operation.updated_at = Some(OffsetDateTime::now_utc());
    persist_site_replication_repair_operation(&operation).await?;

    let tasks = site_replication_repair_tasks(&plan);
    for peer in state.peers.values().filter(|peer| {
        peer.deployment_id != request.local_peer.deployment_id
            && !same_identity_endpoint(&peer.endpoint, &request.local_peer.endpoint)
    }) {
        let transport = match PeerTransport::for_runtime_peer(peer).await {
            Ok(transport) => transport,
            Err(err) => {
                let error = err.to_string();
                for (family_index, task) in &tasks {
                    if !site_replication_repair_task_pending(&operation, &peer.deployment_id, task.family(), *family_index)? {
                        continue;
                    }
                    update_site_replication_repair_task(
                        &mut operation,
                        &peer.deployment_id,
                        task.family(),
                        *family_index,
                        Err(&error),
                    )?;
                    summarize_site_replication_repair_operation(&mut operation);
                    persist_site_replication_repair_task(&operation, peer, task.family(), task.path()).await?;
                }
                continue;
            }
        };

        for (family_index, task) in &tasks {
            if !site_replication_repair_task_pending(&operation, &peer.deployment_id, task.family(), *family_index)? {
                continue;
            }
            let result = task
                .send(&transport, &state.service_account_access_key, &service_account_secret_key)
                .await;
            let error = result.err().map(|err| err.to_string());
            update_site_replication_repair_task(
                &mut operation,
                &peer.deployment_id,
                task.family(),
                *family_index,
                match error.as_deref() {
                    Some(error) => Err(error),
                    None => Ok(()),
                },
            )?;
            summarize_site_replication_repair_operation(&mut operation);
            persist_site_replication_repair_task(&operation, peer, task.family(), task.path()).await?;
        }
    }

    summarize_site_replication_repair_operation(&mut operation);
    persist_site_replication_repair_operation(&operation).await?;
    json_response(&site_replication_repair_operation_response(&operation))
}

pub async fn site_replication_make_bucket_hook(bucket: &str, lock_enabled: bool) -> S3Result<()> {
    let _bucket_op_guard = SITE_REPLICATION_BUCKET_OP_LOCK.read().await;
    let runtime = {
        // The bucket-op lock is what orders this against add/remove. The
        // state is only read here (through the runtime snapshot), and the
        // bucket setup below writes bucket metadata, never the state object —
        // holding the state transaction across it would put local metadata
        // IO inside a distributed lock for nothing.
        let Some(runtime) = runtime_site_replication_targets().await? else {
            return Ok(());
        };

        ensure_site_replication_bucket_versioning(bucket).await?;
        ensure_site_replication_bucket_setup_with_runtime(bucket, &runtime).await?;
        runtime
    };

    broadcast_site_replication_make_bucket(bucket, lock_enabled, Some(&runtime), None).await
}

async fn broadcast_site_replication_json_using_runtime<T: Serialize>(
    runtime: Option<&SiteReplicationRuntime>,
    path: &str,
    body: &T,
) -> S3Result<()> {
    match runtime {
        Some(runtime) => broadcast_site_replication_json_with_runtime(runtime, path, body).await,
        None => broadcast_site_replication_json(path, body).await,
    }
}

async fn broadcast_site_replication_make_bucket(
    bucket: &str,
    lock_enabled: bool,
    runtime: Option<&SiteReplicationRuntime>,
    bootstrap_token: Option<&str>,
) -> S3Result<()> {
    let created_at = current_object_store_handle()
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
    let path = if let Some(token) = bootstrap_token {
        with_site_replication_bootstrap_token(&path, token)
    } else {
        path
    };
    broadcast_site_replication_json_using_runtime(runtime, &path, &serde_json::json!({})).await?;

    let configure_path = bootstrap_bucket_op_path(bucket, "configure-replication");
    let configure_path = if let Some(token) = bootstrap_token {
        with_site_replication_bootstrap_token(&configure_path, token)
    } else {
        configure_path
    };
    broadcast_site_replication_json_using_runtime(runtime, &configure_path, &serde_json::json!({})).await
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
    let Some(store) = current_object_store_handle() else {
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
            location: current_region().map(|region| region.to_string()).unwrap_or_default(),
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
            entry.replication_targets_online =
                Some(site_replication_targets_online(&bucket.name, &metadata.replication_config_xml).await);
        }

        info.buckets.insert(bucket.name, entry);
    }

    if let Some(iam_sys) = current_iam_handle() {
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
    if let Some(federation) = current_federated_identity_service() {
        let providers = federation.list_providers();
        settings.open_id.enabled = !providers.is_empty();
        settings.open_id.region = current_region().map(|region| region.to_string()).unwrap_or_default();

        for provider in providers {
            let Some(config) = federation.get_provider_config(&provider.provider_id) else {
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
        user_type: sr_wire_user_type(user_type, is_group),
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
    let Some(stats) = current_replication_stats_handle() else {
        return SRMetricsSummary::default();
    };

    let node = stats.site_metrics_snapshot().await;
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
            curr: node.active_workers_curr,
            avg: node.active_workers_avg,
            max: node.active_workers_max,
        },
        replica_size: node.replica_size,
        replica_count: node.replica_count,
        queued: InQueueMetric {
            curr: qstat(node.queued_curr_count, node.queued_curr_bytes),
            avg: qstat(node.queued_avg_count, node.queued_avg_bytes),
            max: qstat(node.queued_max_count, node.queued_max_bytes),
        },
        in_progress: InProgressMetric::default(),
        proxied: ReplProxyMetric {
            get_total: non_negative_u64(node.proxy_get_total),
            head_total: non_negative_u64(node.proxy_head_total),
            get_failed_total: non_negative_u64(node.proxy_get_failed),
            head_failed_total: non_negative_u64(node.proxy_head_failed),
            put_tag_total: non_negative_u64(node.proxy_put_tag_total),
            put_tag_failed_total: non_negative_u64(node.proxy_put_tag_failed),
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
        &runtime_peer_connection(peer)?,
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

fn site_replication_rule_complete(rule: &ReplicationRule, owner_deployment_id: &str) -> bool {
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

    // A rule whose destination ARN names the site that holds it can never replicate:
    // `reconcile_site_replication_bucket_targets` skips the local peer, so no bucket
    // target backs that ARN and every object is dropped. Two sites holding byte-identical
    // configs used to satisfy this check while exactly one of them could push.
    let points_at_remote_site = replication_target_arn_deployment_id(&rule.destination.bucket)
        .is_some_and(|deployment_id| deployment_id != owner_deployment_id);

    rule.id.as_deref().is_some_and(|id| id.starts_with("site-repl-"))
        && rule.status == ReplicationRuleStatus::from_static(ReplicationRuleStatus::ENABLED)
        && points_at_remote_site
        && delete_marker_enabled
        && delete_enabled
        && existing_object_enabled
        && replica_modifications_enabled
}

fn site_replication_config_mismatch<'a>(
    values: impl Iterator<Item = (&'a str, Option<&'a String>)>,
    total_sites: usize,
) -> (usize, bool) {
    let values = values
        .filter_map(|(deployment_id, value)| value.map(|value| (deployment_id, value)))
        .collect::<Vec<_>>();
    let present = values.len();
    if present == 0 {
        return (0, false);
    }
    if present != total_sites {
        return (present, true);
    }

    let expected_rules = total_sites.saturating_sub(1);
    let replicated = values.iter().all(|(deployment_id, raw)| {
        // `raw` is the wire form produced by build_sr_info, i.e. base64-encoded XML
        // (raw_config_to_base64). Decode it before XML-parsing — parsing the base64 text
        // directly always fails, which would falsely report every replicated bucket as
        // out-of-sync ("0/N Buckets in sync"). decode_bucket_meta_wire_value falls back to
        // the raw bytes when the value is not base64, so plain-XML callers still work.
        let xml = decode_bucket_meta_wire_value(raw);
        deserialize::<ReplicationConfiguration>(&xml).is_ok_and(|config| {
            config.rules.len() == expected_rules
                && config
                    .rules
                    .iter()
                    .all(|rule| site_replication_rule_complete(rule, deployment_id))
        })
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
        let (_, rules_mismatch) = site_replication_config_mismatch(
            site_infos.iter().map(|(deployment_id, info)| {
                (
                    deployment_id.as_str(),
                    info.buckets
                        .get(bucket_name)
                        .and_then(|bucket| bucket.replication_config.as_ref()),
                )
            }),
            total_sites,
        );
        // Well-formed rules on a site that cannot reach the peer behind them replicate
        // nothing, so a site reporting an offline target is out of sync regardless of how
        // its rule set reads. Peers that do not report the field are left out of the verdict.
        let targets_offline = site_infos.values().any(|info| {
            info.buckets
                .get(bucket_name)
                .and_then(|bucket| bucket.replication_targets_online)
                == Some(false)
        });
        let replication_mismatch = rules_mismatch || targets_offline;
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

fn peer_edit_identity_is_empty(peer: &PeerInfo) -> bool {
    peer.deployment_id.is_empty() && peer.endpoint.is_empty() && peer.name.is_empty()
}

fn peer_edit_has_non_identity_payload(peer: &PeerInfo) -> bool {
    peer.sync_state != SyncStatus::Unknown
        || peer.default_bandwidth.limit != 0
        || peer.default_bandwidth.set
        || peer.default_bandwidth.updated_at.is_some()
        || peer.replicate_ilm_expiry
        || !peer.object_naming_mode.is_empty()
        || peer.skip_tls_verify
        || !peer.ca_cert_pem.is_empty()
        || peer.api_version.is_some()
}

fn apply_internal_peer_edit(
    state: SiteReplicationState,
    local_peer: &PeerInfo,
    incoming: PeerInfo,
    ilm_expiry_override: Option<bool>,
) -> S3Result<SiteReplicationState> {
    if peer_edit_identity_is_empty(&incoming) {
        if ilm_expiry_override.is_none() || peer_edit_has_non_identity_payload(&incoming) {
            return Err(s3_error!(InvalidRequest, "peer identity is required"));
        }
        return Ok(edit_state(state, incoming, ilm_expiry_override));
    }

    validate_proposed_peer(&incoming)?;
    Ok(sync_state_name_for_local_peer(
        update_peer(state, incoming.clone(), ilm_expiry_override),
        local_peer,
        &incoming,
    ))
}

fn peer_endpoint_edit_requested(state: &SiteReplicationState, incoming: &PeerInfo) -> bool {
    !incoming.deployment_id.is_empty() && !incoming.endpoint.is_empty() && state.peers.contains_key(&incoming.deployment_id)
}

fn peer_connection_settings_match(left: &PeerInfo, right: &PeerInfo) -> bool {
    canonical_endpoint(&left.endpoint) == canonical_endpoint(&right.endpoint)
        && left.skip_tls_verify == right.skip_tls_verify
        && left.ca_cert_pem.trim() == right.ca_cert_pem.trim()
}

fn peer_endpoint_refresh_requested(state: &SiteReplicationState, incoming: &PeerInfo) -> bool {
    if !peer_endpoint_edit_requested(state, incoming) {
        return false;
    }
    if let Some(pending) = pending_endpoint_refresh(state) {
        return pending.peer.deployment_id == incoming.deployment_id && peer_connection_settings_match(&pending.peer, incoming);
    }
    state
        .peers
        .get(&incoming.deployment_id)
        .is_some_and(|peer| !peer_connection_settings_match(peer, incoming))
}

fn pending_endpoint_refresh(state: &SiteReplicationState) -> Option<PendingEndpointRefresh> {
    state.pending_endpoint_refresh.clone().or_else(|| {
        state
            .retry_queue
            .iter()
            .find(|event| event.path == SITE_REPLICATION_ENDPOINT_REFRESH_RETRY_PATH)
            .and_then(|event| serde_json::from_str(&event.last_error).ok())
    })
}

fn merge_pending_endpoint_refresh(
    state: &SiteReplicationState,
    candidate: &PendingEndpointRefresh,
    acked_deployment_ids: impl IntoIterator<Item = String>,
) -> S3Result<PendingEndpointRefresh> {
    let mut merged = if let Some(latest) = pending_endpoint_refresh(state) {
        if latest.id != candidate.id
            || latest.peer.deployment_id != candidate.peer.deployment_id
            || !peer_connection_settings_match(&latest.peer, &candidate.peer)
        {
            return Err(s3_error!(InvalidRequest, "endpoint target refresh state changed during update"));
        }
        latest
    } else {
        candidate.clone()
    };
    merged
        .acked_deployment_ids
        .extend(candidate.acked_deployment_ids.iter().cloned());
    merged.acked_deployment_ids.extend(acked_deployment_ids);
    Ok(merged)
}

fn internal_endpoint_refresh_already_committed(state: &SiteReplicationState, incoming: &PeerInfo) -> bool {
    pending_endpoint_refresh(state).is_none()
        && state
            .peers
            .get(&incoming.deployment_id)
            .is_some_and(|committed| peer_connection_settings_match(committed, incoming))
}

/// An admin add/edit's precondition, re-evaluated inside the transaction that
/// is about to commit: the topology must still be the one the operation was
/// planned against, and the endpoint refresh must still be the same one (or
/// still absent). The planning snapshot is taken before peer probes and
/// fan-outs, none of which may hold the state-object lock, so only the check
/// inside the committing closure binds — the same check between network
/// stages is advisory, fencing the common race off the side-effect path.
/// `stage` names what was in flight for the operator; a rejected commit is
/// safe to re-run.
fn ensure_edit_precondition(
    state: &SiteReplicationState,
    expected_updated_at: Option<OffsetDateTime>,
    expected_pending_id: Option<&String>,
    stage: &str,
) -> S3Result<()> {
    if state.updated_at != expected_updated_at
        || pending_endpoint_refresh(state).as_ref().map(|pending| &pending.id) != expected_pending_id
    {
        return Err(s3_error!(InvalidRequest, "site replication state changed during {stage}"));
    }
    Ok(())
}

fn set_pending_endpoint_refresh(state: &mut SiteReplicationState, pending: PendingEndpointRefresh) -> S3Result<()> {
    state
        .retry_queue
        .retain(|event| event.path != SITE_REPLICATION_ENDPOINT_REFRESH_RETRY_PATH);
    state.retry_queue.push(SiteReplicationRetryEvent {
        id: pending.id.clone(),
        peer_deployment_id: pending.peer.deployment_id.clone(),
        peer_endpoint: pending.peer.endpoint.clone(),
        path: SITE_REPLICATION_ENDPOINT_REFRESH_RETRY_PATH.to_string(),
        retry_count: 0,
        failed: false,
        last_error: "endpoint target refresh pending".to_string(),
        updated_at: Some(OffsetDateTime::now_utc()),
        edit_generation: None,
    });
    state.pending_endpoint_refresh = Some(pending);
    Ok(())
}

fn clear_pending_endpoint_refresh(state: &mut SiteReplicationState) {
    state.pending_endpoint_refresh = None;
    state
        .retry_queue
        .retain(|event| event.path != SITE_REPLICATION_ENDPOINT_REFRESH_RETRY_PATH);
}

fn endpoint_refresh_target_state(state: &SiteReplicationState, pending: &PendingEndpointRefresh) -> SiteReplicationState {
    let mut target_state = state.clone();
    let peer = normalize_peer_info(pending.peer.clone());
    target_state.peers.insert(peer.deployment_id.clone(), peer);
    target_state
}

fn parse_endpoint_refresh_status(peer: &PeerInfo, body: &[u8]) -> S3Result<()> {
    let status: ReplicateEditStatus = serde_json::from_slice(body).map_err(|_| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("peer {} does not support endpoint target refresh", peer.endpoint),
        )
    })?;
    if status.success {
        Ok(())
    } else {
        Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("peer {} failed endpoint target refresh: {}", peer.endpoint, status.err_detail),
        ))
    }
}

fn endpoint_refresh_capability_supported(peer: &PeerInfo, status: StatusCode, body: &[u8]) -> S3Result<bool> {
    peer_capability_response_supported(peer, status, body)
}

fn peer_capability_response_supported(peer: &PeerInfo, status: StatusCode, body: &[u8]) -> S3Result<bool> {
    if status.is_success() {
        return Ok(parse_endpoint_refresh_status(peer, body).is_ok());
    }
    if matches!(status, StatusCode::BAD_REQUEST | StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED) {
        return Ok(false);
    }

    Err(S3Error::with_message(
        S3ErrorCode::InternalError,
        format!("probe site replication capability on peer {} failed with {status}", peer.endpoint),
    ))
}

async fn require_add_peer_tls_capability(sites: &[PeerSite], local_peer: &PeerInfo) -> S3Result<()> {
    if !add_peer_tls_capability_required(sites) {
        return Ok(());
    }

    let remote_sites = peer_tls_capability_probe_sites(sites)
        .into_iter()
        .filter(|site| !same_identity_endpoint(&site.endpoint, &local_peer.endpoint))
        .collect::<Vec<_>>();
    let probes = futures::future::join_all(remote_sites.iter().map(|site| async move {
        let connection = PeerConnection::try_from(*site)?;
        let client = site_replication_client_for(&connection).await?;
        send_peer_admin_request_raw_with_client(
            &client,
            &connection,
            SITE_REPLICATION_PEER_TLS_CAPABILITY_PATH,
            &site.access_key,
            &site.secret_key,
            &(),
        )
        .await
    }))
    .await;
    for (site, probe) in remote_sites.into_iter().zip(probes) {
        let (status, body) = probe?;
        let peer = normalize_peer_site(site.clone(), false);
        if !peer_capability_response_supported(&peer, status, &body)? {
            return Err(s3_error!(
                InvalidRequest,
                "site `{}` does not support site replication TLS settings",
                site.endpoint
            ));
        }
    }
    Ok(())
}

async fn require_edit_peer_tls_capability(
    state: &SiteReplicationState,
    proposed: &PeerInfo,
    local_peer: &PeerInfo,
    access_key: &str,
    secret_key: &str,
) -> S3Result<()> {
    let existing = existing_peer_for_edit(state, proposed);
    if !edit_peer_tls_capability_required(existing, proposed) {
        return Ok(());
    }

    let mut route_peer = proposed.clone();
    if let Some(existing) = existing
        && route_peer.deployment_id != existing.deployment_id
    {
        route_peer.deployment_id = existing.deployment_id.clone();
    }
    let routes = PendingEndpointRefresh {
        peer: route_peer,
        ..Default::default()
    };
    let targets = state
        .peers
        .values()
        .filter(|target| target.deployment_id != local_peer.deployment_id)
        .collect::<Vec<_>>();
    let probes = futures::future::join_all(targets.iter().map(|target| {
        send_endpoint_refresh_admin_request_raw(
            target,
            &routes,
            SITE_REPLICATION_PEER_TLS_CAPABILITY_PATH,
            access_key,
            secret_key,
            &(),
        )
    }))
    .await;
    for (target, probe) in targets.into_iter().zip(probes) {
        let (status, body) = probe?;
        if !peer_capability_response_supported(target, status, &body)? {
            return Err(s3_error!(
                InvalidRequest,
                "site `{}` does not support site replication TLS settings",
                target.endpoint
            ));
        }
    }
    Ok(())
}

async fn probe_proposed_peer_tls_transport(peer: &PeerInfo, access_key: &str, secret_key: &str) -> S3Result<()> {
    let connection = PeerConnection::try_from(peer)?;
    let client = site_replication_client_for(&connection).await?;
    let (status, body) = send_peer_admin_request_raw_with_client(
        &client,
        &connection,
        SITE_REPLICATION_PEER_TLS_CAPABILITY_PATH,
        access_key,
        secret_key,
        &(),
    )
    .await?;
    if peer_capability_response_supported(peer, status, &body)? {
        Ok(())
    } else {
        Err(s3_error!(
            InvalidRequest,
            "site `{}` does not support site replication TLS settings",
            peer.endpoint
        ))
    }
}

fn endpoint_refresh_route_endpoints(target: &PeerInfo, pending: &PendingEndpointRefresh) -> S3Result<Vec<PeerConnection>> {
    let mut endpoints = Vec::new();
    let mut invalid = None;
    match runtime_peer_connection(target) {
        Ok(connection) => endpoints.push(connection),
        Err(err) => invalid = Some(err),
    }
    if target.deployment_id == pending.peer.deployment_id {
        match runtime_peer_connection(&pending.peer) {
            Ok(connection) if !endpoints.contains(&connection) => endpoints.push(connection),
            Ok(_) => {}
            Err(err) if invalid.is_none() => invalid = Some(err),
            Err(_) => {}
        }
    }
    if endpoints.is_empty() {
        return Err(invalid.unwrap_or_else(|| {
            S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("site replication peer `{}` has no usable endpoint", target.endpoint),
            )
        }));
    }
    Ok(endpoints)
}

async fn endpoint_refresh_route_transports(target: &PeerInfo, pending: &PendingEndpointRefresh) -> S3Result<Vec<PeerTransport>> {
    let mut transports = Vec::new();
    let mut first_error = None;
    for connection in endpoint_refresh_route_endpoints(target, pending)? {
        match site_replication_client_for(&connection).await {
            Ok(client) => transports.push(PeerTransport { connection, client }),
            Err(err) if first_error.is_none() => {
                first_error = Some(S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("initialize persisted site replication peer `{}` transport failed: {err}", target.endpoint),
                ));
            }
            Err(_) => {}
        }
    }
    if transports.is_empty() {
        return Err(first_error.unwrap_or_else(|| {
            S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("site replication peer `{}` has no usable transport", target.endpoint),
            )
        }));
    }
    Ok(transports)
}

fn endpoint_refresh_remote_targets<'a>(
    routing_peers: &'a BTreeMap<String, PeerInfo>,
    pending: Option<&PendingEndpointRefresh>,
    local_deployment_id: Option<&str>,
) -> Vec<&'a PeerInfo> {
    routing_peers
        .values()
        .filter(|target| {
            local_deployment_id.is_none_or(|deployment_id| deployment_id != target.deployment_id)
                && pending.is_none_or(|pending| !pending.acked_deployment_ids.contains(&target.deployment_id))
        })
        .collect()
}

async fn send_endpoint_refresh_admin_request<T: Serialize>(
    target: &PeerInfo,
    pending: &PendingEndpointRefresh,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<Vec<u8>> {
    let (status, response) = send_endpoint_refresh_admin_request_raw(target, pending, path, access_key, secret_key, body).await?;
    endpoint_refresh_response(target, status, response)
}

async fn send_endpoint_refresh_admin_request_with_transports<T: Serialize>(
    target: &PeerInfo,
    transports: &[PeerTransport],
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<Vec<u8>> {
    let (status, response) =
        send_endpoint_refresh_admin_request_raw_with_transports(target, transports, path, access_key, secret_key, body).await?;
    endpoint_refresh_response(target, status, response)
}

fn endpoint_refresh_response(target: &PeerInfo, status: StatusCode, response: Vec<u8>) -> S3Result<Vec<u8>> {
    if status.is_success() {
        return Ok(response);
    }

    Err(S3Error::with_message(
        S3ErrorCode::InternalError,
        format!(
            "peer {} endpoint target refresh failed with {status}: {}",
            target.endpoint,
            String::from_utf8_lossy(&response)
        ),
    ))
}

async fn send_endpoint_refresh_admin_request_raw<T: Serialize>(
    target: &PeerInfo,
    pending: &PendingEndpointRefresh,
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<(StatusCode, Vec<u8>)> {
    let transports = endpoint_refresh_route_transports(target, pending).await?;
    send_endpoint_refresh_admin_request_raw_with_transports(target, &transports, path, access_key, secret_key, body).await
}

async fn send_endpoint_refresh_admin_request_raw_with_transports<T: Serialize>(
    target: &PeerInfo,
    transports: &[PeerTransport],
    path: &str,
    access_key: &str,
    secret_key: &str,
    body: &T,
) -> S3Result<(StatusCode, Vec<u8>)> {
    let mut last_error = None;
    let mut last_response = None;
    for transport in transports {
        match send_peer_admin_request_raw_with_client(
            &transport.client,
            &transport.connection,
            path,
            access_key,
            secret_key,
            body,
        )
        .await
        {
            Ok((status, response))
                if matches!(status, StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED | StatusCode::GONE)
                    || status.is_server_error() =>
            {
                last_response = Some((status, response));
            }
            Ok(response) => return Ok(response),
            Err(err) => last_error = Some(err),
        }
    }

    if let Some(response) = last_response {
        return Ok(response);
    }
    Err(last_error.unwrap_or_else(|| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("peer {} endpoint target refresh failed", target.endpoint),
        )
    }))
}

async fn legacy_peer_bucket_names_with_transports(
    target: &PeerInfo,
    transports: &[PeerTransport],
    access_key: &str,
    secret_key: &str,
) -> S3Result<Vec<String>> {
    let mut last_error = None;
    for transport in transports {
        match send_peer_admin_get_request_with_client(
            &transport.client,
            &transport.connection,
            "/rustfs/admin/v3/site-replication/metainfo?buckets=true",
            access_key,
            secret_key,
        )
        .await
        {
            Ok(body) => return peer_bucket_names_from_metainfo(transport.connection.endpoint(), &body),
            Err(err) => last_error = Some(err),
        }
    }

    Err(last_error.unwrap_or_else(|| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("list site replication buckets on peer {} failed", target.endpoint),
        )
    }))
}

fn peer_bucket_names_from_metainfo(endpoint: &str, body: &[u8]) -> S3Result<Vec<String>> {
    let info: Value = serde_json::from_slice(body).map_err(|err| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("parse site replication metainfo from {endpoint} failed: {err}"),
        )
    })?;
    let Some(buckets) = info.get("buckets").or_else(|| info.get("Buckets")) else {
        return Ok(Vec::new());
    };
    let buckets = buckets.as_object().ok_or_else(|| {
        S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("site replication metainfo from {endpoint} has invalid buckets"),
        )
    })?;
    Ok(buckets.keys().cloned().collect())
}

async fn refresh_legacy_peer_bucket_targets(
    target: &PeerInfo,
    pending: &PendingEndpointRefresh,
    access_key: &str,
    secret_key: &str,
) -> S3Result<()> {
    let transports = endpoint_refresh_route_transports(target, pending).await?;
    send_endpoint_refresh_admin_request_with_transports(
        target,
        &transports,
        SITE_REPLICATION_PEER_EDIT_PATH,
        access_key,
        secret_key,
        &pending.peer,
    )
    .await?;

    let buckets = legacy_peer_bucket_names_with_transports(target, &transports, access_key, secret_key).await?;
    let mut configure_operation = None;
    for bucket in &buckets {
        if let Some(operation) = configure_operation {
            let path = bootstrap_bucket_op_path(bucket, operation);
            send_endpoint_refresh_admin_request_with_transports(
                target,
                &transports,
                &path,
                access_key,
                secret_key,
                &serde_json::json!({}),
            )
            .await?;
            continue;
        }

        let minio_path = bootstrap_bucket_op_path(bucket, "ConfigureReplication");
        let (status, _) = send_endpoint_refresh_admin_request_raw_with_transports(
            target,
            &transports,
            &minio_path,
            access_key,
            secret_key,
            &serde_json::json!({}),
        )
        .await?;
        if status.is_success() {
            configure_operation = Some("ConfigureReplication");
            continue;
        }

        let rustfs_path = bootstrap_bucket_op_path(bucket, SITE_REPLICATION_BUCKET_OP_CONFIGURE_REPLICATION);
        send_endpoint_refresh_admin_request_with_transports(
            target,
            &transports,
            &rustfs_path,
            access_key,
            secret_key,
            &serde_json::json!({}),
        )
        .await?;
        configure_operation = Some(SITE_REPLICATION_BUCKET_OP_CONFIGURE_REPLICATION);
    }

    Ok(())
}

fn align_peer_edit_deployment_id(state: &SiteReplicationState, incoming: &mut PeerInfo) {
    if incoming.name.is_empty() || state.peers.contains_key(&incoming.deployment_id) {
        return;
    }

    let mut matches = state.peers.values().filter(|peer| peer.name == incoming.name);
    let Some(peer) = matches.next() else {
        return;
    };
    if matches.next().is_none() {
        incoming.deployment_id = peer.deployment_id.clone();
    }
}

fn remove_sites(mut state: SiteReplicationState, req: SRRemoveReq) -> SiteReplicationState {
    if req.remove_all {
        state.peers.clear();
        state.resync_status.clear();
        state.retry_queue.clear();
        state.pending_endpoint_refresh = None;
        state.updated_at = Some(OffsetDateTime::now_utc());
        return state;
    }

    let names: HashSet<String> = req.site_names.into_iter().collect();
    if names.contains(&state.name) {
        state.peers.clear();
        state.resync_status.clear();
        state.retry_queue.clear();
        state.pending_endpoint_refresh = None;
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
    if state
        .pending_endpoint_refresh
        .as_ref()
        .is_some_and(|pending| !state.peers.contains_key(&pending.peer.deployment_id))
    {
        clear_pending_endpoint_refresh(&mut state);
    }
    state.updated_at = Some(OffsetDateTime::now_utc());
    state
}

fn removed_deployment_ids_for_remove_req(state: &SiteReplicationState, req: &SRRemoveReq) -> HashSet<String> {
    if req.remove_all || req.site_names.contains(&state.name) {
        return state.peers.keys().cloned().collect();
    }

    let names: HashSet<&str> = req.site_names.iter().map(String::as_str).collect();
    state
        .peers
        .values()
        .filter(|peer| names.contains(peer.name.as_str()))
        .map(|peer| peer.deployment_id.clone())
        .collect()
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

/// Allocate the next peer-edit generation. Called inside the state
/// transaction, so the counter is handed out under the distributed
/// state-object lock and two nodes of this site can never take the same one.
fn next_peer_edit_generation(state: &mut SiteReplicationState) -> u64 {
    state.edit_generation = state.edit_generation.saturating_add(1);
    state.edit_generation
}

/// Build the peer-edit request path carrying the fencing token. The bare
/// constant stays the retry-queue key: the query only fences the wire
/// delivery, and a per-generation key would make every retry event unique.
/// Without a local deployment id there is nothing to fence against, so the
/// unstamped path is sent and the receiver keeps its pre-fence behaviour.
fn peer_edit_path_with_fence(origin: Option<&str>, generation: u64) -> String {
    let Some(origin) = origin.filter(|origin| !origin.is_empty()) else {
        return SITE_REPLICATION_PEER_EDIT_PATH.to_string();
    };
    let query = form_urlencoded::Serializer::new(String::new())
        .append_pair(SITE_REPLICATION_EDIT_ORIGIN_QUERY, origin)
        .append_pair(SITE_REPLICATION_EDIT_GENERATION_QUERY, &generation.to_string())
        .finish();
    format!("{SITE_REPLICATION_PEER_EDIT_PATH}?{query}")
}

/// The (origin site, generation) fence an incoming peer edit carries, when the
/// sender stamped one. An unstamped edit (older peer) has no fence and is
/// applied as before.
fn peer_edit_fence(queries: &HashMap<String, String>) -> Option<(String, u64)> {
    let origin = queries
        .get(SITE_REPLICATION_EDIT_ORIGIN_QUERY)
        .filter(|origin| !origin.is_empty())?;
    let generation = queries.get(SITE_REPLICATION_EDIT_GENERATION_QUERY)?.parse::<u64>().ok()?;
    Some((origin.clone(), generation))
}

/// True when a strictly newer edit from the same origin site already landed
/// here. No lock on the sending side can order deliveries issued by two
/// nodes of that site, so ordering is decided here, on the generation the
/// sender allocated under the distributed lock. Equal generations are NOT
/// stale: one edit legitimately fans out several deliveries under a single
/// generation (the ILM-expiry edit sends every peer's record), and a replay of
/// an applied delivery re-applies the same edit idempotently.
fn peer_edit_delivery_is_stale(state: &SiteReplicationState, origin: &str, generation: u64) -> bool {
    state
        .applied_edit_generations
        .get(origin)
        .is_some_and(|applied| *applied > generation)
}

fn record_applied_peer_edit_generation(state: &mut SiteReplicationState, origin: &str, generation: u64) {
    let applied = state.applied_edit_generations.entry(origin.to_string()).or_default();
    *applied = (*applied).max(generation);
}

fn retry_event_matches(event: &SiteReplicationRetryEvent, peer: &PeerInfo, path: &str) -> bool {
    (event.peer_deployment_id == peer.deployment_id || event.peer_endpoint == peer.endpoint) && event.path == path
}

fn dequeue_site_replication_retry_events(queue: &mut Vec<SiteReplicationRetryEvent>, peer: &PeerInfo, path: &str) -> usize {
    settle_site_replication_retry_events(queue, peer, path, None)
}

/// Remove the retry events for (peer, path) that `generation` is entitled to
/// settle. A successful delivery only proves the peer reached the state the
/// delivery carried: while it was in flight another edit can commit, fail its
/// own delivery, and enqueue for the same (peer, path). Erasing that event
/// would leave the peer on the older edit with no retry left, so an event
/// stamped with a NEWER generation survives. `None` settles unconditionally —
/// the broadcast paths that carry no generation, whose retry events live under
/// their own paths and never collide with peer-edit deliveries.
fn settle_site_replication_retry_events(
    queue: &mut Vec<SiteReplicationRetryEvent>,
    peer: &PeerInfo,
    path: &str,
    generation: Option<u64>,
) -> usize {
    let before = queue.len();
    queue.retain(|event| {
        if !retry_event_matches(event, peer, path) {
            return true;
        }
        match (generation, event.edit_generation) {
            (Some(settled), Some(failed)) => failed > settled,
            _ => false,
        }
    });
    before.saturating_sub(queue.len())
}

fn upsert_site_replication_retry_event(
    queue: &mut Vec<SiteReplicationRetryEvent>,
    peer: &PeerInfo,
    path: &str,
    error: &str,
    generation: Option<u64>,
) {
    let now = OffsetDateTime::now_utc();
    let detail = summarize_peer_error_detail(error);
    if let Some(event) = queue.iter_mut().find(|event| retry_event_matches(event, peer, path)) {
        event.retry_count = event.retry_count.saturating_add(1);
        event.failed = event.retry_count >= SITE_REPLICATION_RETRY_FAILED_AFTER;
        event.last_error = detail;
        event.updated_at = Some(now);
        // Keep the newest generation: an older delivery that fails afterwards
        // must not lower the fence and let its own success settle the event.
        event.edit_generation = event.edit_generation.max(generation);
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
        edit_generation: generation,
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
    enqueue_site_replication_retry_event_for_generation(peer, path, error, None).await
}

async fn enqueue_site_replication_retry_event_for_generation(
    peer: &PeerInfo,
    path: &str,
    error: &S3Error,
    generation: Option<u64>,
) {
    let peer_owned = peer.clone();
    let path_owned = path.to_string();
    let error_text = error.to_string();
    let result = update_site_replication_state(move |state| {
        upsert_site_replication_retry_event(&mut state.retry_queue, &peer_owned, &path_owned, &error_text, generation);
        Ok(())
    })
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

fn retry_bucket_operation(path: &str) -> Option<String> {
    let (base_path, query) = path.split_once('?')?;
    if base_path != SITE_REPLICATION_PEER_BUCKET_OPS_PATH {
        return None;
    }

    form_urlencoded::parse(query.as_bytes()).find_map(|(key, value)| (key == "operation").then(|| value.into_owned()))
}

fn retry_event_replayed_by_bootstrap(event: &SiteReplicationRetryEvent) -> bool {
    matches!(
        retry_bucket_operation(&event.path).as_deref(),
        Some(SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING | SITE_REPLICATION_BUCKET_OP_CONFIGURE_REPLICATION)
    )
}

/// Remove a retry event for (peer, path) from the queue on successful delivery.
/// This is a no-op (load + no-op persist skipped) when no matching entry exists,
/// avoiding unnecessary I/O on the common path.
async fn dequeue_site_replication_retry_event(peer: &PeerInfo, path: &str) {
    dequeue_site_replication_retry_event_for_generation(peer, path, None).await
}

async fn dequeue_site_replication_retry_event_for_generation(peer: &PeerInfo, path: &str, generation: Option<u64>) {
    let result = async {
        // Fast path: this sits on every successful hook broadcast, so probe
        // with a plain read first and only enter the locked RMW on a hit
        // (the transaction re-checks under the lock).
        let mut probe = load_site_replication_state().await?;
        if settle_site_replication_retry_events(&mut probe.retry_queue, peer, path, generation) == 0 {
            return Ok(());
        }
        let peer_owned = peer.clone();
        let path_owned = path.to_string();
        update_site_replication_state(move |state| {
            settle_site_replication_retry_events(&mut state.retry_queue, &peer_owned, &path_owned, generation);
            Ok(())
        })
        .await?;
        Ok::<_, S3Error>(())
    }
    .await;

    if let Err(err) = result {
        warn!(
            component = LOG_COMPONENT_ADMIN,
            subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
            event = EVENT_ADMIN_SITE_REPLICATION_STATE,
            peer = %peer.endpoint,
            deployment_id = %peer.deployment_id,
            path,
            error = ?err,
            "failed to dequeue site replication retry event"
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

    let rotation_id = rotation_id.to_string();
    update_site_replication_state_when_changed(move |state| {
        let Some(pending) = state.pending_rotation.as_mut().filter(|pending| pending.id == rotation_id) else {
            return Ok(StateCommit::Unchanged(()));
        };
        push_unique_secret_candidate(&mut pending.secret_candidates, secret);
        Ok(StateCommit::Changed(()))
    })
    .await
}

async fn record_pending_remove_secret_candidate(remove_id: &str, secret: String) -> S3Result<()> {
    if secret.is_empty() {
        return Ok(());
    }

    let remove_id = remove_id.to_string();
    update_site_replication_state_when_changed(move |state| {
        let Some(pending) = state.pending_remove.as_mut().filter(|pending| pending.id == remove_id) else {
            return Ok(StateCommit::Unchanged(()));
        };
        push_unique_secret_candidate(&mut pending.secret_candidates, secret);
        Ok(StateCommit::Changed(()))
    })
    .await
}

async fn mark_pending_rotation_peer_acked(rotation_id: &str, deployment_id: &str) -> S3Result<()> {
    let rotation_id = rotation_id.to_string();
    let deployment_id = deployment_id.to_string();
    update_site_replication_state_when_changed(move |state| {
        let Some(pending) = state.pending_rotation.as_mut().filter(|pending| pending.id == rotation_id) else {
            return Ok(StateCommit::Unchanged(()));
        };
        pending.acked_deployment_ids.insert(deployment_id);
        Ok(StateCommit::Changed(()))
    })
    .await
}

async fn mark_pending_remove_peer_acked(remove_id: &str, deployment_id: &str) -> S3Result<()> {
    let remove_id = remove_id.to_string();
    let deployment_id = deployment_id.to_string();
    update_site_replication_state_when_changed(move |state| {
        let Some(pending) = state.pending_remove.as_mut().filter(|pending| pending.id == remove_id) else {
            return Ok(StateCommit::Unchanged(()));
        };
        pending.acked_deployment_ids.insert(deployment_id);
        Ok(StateCommit::Changed(()))
    })
    .await
}

async fn finalize_pending_rotation_if_complete(rotation_id: &str, local_peer: &PeerInfo) -> S3Result<bool> {
    let rotation_id = rotation_id.to_string();
    let local_peer = local_peer.clone();
    update_site_replication_state_when_changed(move |state| {
        let Some(pending) = state.pending_rotation.as_ref() else {
            return Ok(StateCommit::Unchanged(true));
        };
        if pending.id != rotation_id {
            return Ok(StateCommit::Unchanged(false));
        }
        if !pending_all_remote_peers_acked(&pending.peers, &local_peer, &pending.acked_deployment_ids) {
            return Ok(StateCommit::Unchanged(false));
        }

        state.pending_rotation = None;
        Ok(StateCommit::Changed(true))
    })
    .await
}

async fn pending_remove_ready_to_finalize(remove_id: &str, local_peer: &PeerInfo) -> S3Result<Option<PendingRemove>> {
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
    let remove_id = remove_id.to_string();
    update_site_replication_state_when_changed(move |state| {
        if state.pending_remove.as_ref().is_none_or(|pending| pending.id != remove_id) {
            return Ok(StateCommit::Unchanged(()));
        }
        state.pending_remove = None;
        Ok(StateCommit::Changed(()))
    })
    .await
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

#[derive(Debug, Serialize, Deserialize)]
struct SiteResyncContinuationToken {
    id: String,
    generation: u64,
    offset: usize,
}

fn site_resync_is_active(status: &SRResyncOpStatus) -> bool {
    matches!(status.state.as_str(), "pending" | "running" | "canceling")
}

fn site_resync_cancel_is_idempotent(status: &SRResyncOpStatus) -> bool {
    status.state == "canceled"
}

fn site_resync_nonnegative(value: i64) -> u64 {
    u64::try_from(value.max(0)).unwrap_or_default()
}

fn site_resync_bucket_state(status: replication::ResyncStatusType) -> &'static str {
    match status {
        replication::ResyncStatusType::ResyncPending => "pending",
        replication::ResyncStatusType::ResyncStarted => "running",
        replication::ResyncStatusType::ResyncCompleted => "completed",
        replication::ResyncStatusType::ResyncCanceled => "canceled",
        replication::ResyncStatusType::ResyncFailed | replication::ResyncStatusType::NoResync => "failed",
    }
}

fn site_bucket_resync_is_active(status: replication::ResyncStatusType) -> bool {
    matches!(
        status,
        replication::ResyncStatusType::ResyncPending | replication::ResyncStatusType::ResyncStarted
    )
}

fn apply_site_resync_target_status(bucket: &mut ResyncBucketStatus, target: &replication::TargetReplicationResyncStatus) {
    bucket.status = site_resync_bucket_state(target.resync_status).to_string();
    bucket.started_at = target.start_time;
    bucket.updated_at = target.last_update;
    bucket.replicated_objects = site_resync_nonnegative(target.replicated_count);
    bucket.replicated_bytes = site_resync_nonnegative(target.replicated_size);
    bucket.failed_objects = site_resync_nonnegative(target.failed_count);
    bucket.failed_bytes = site_resync_nonnegative(target.failed_size);
    bucket.err_detail = target.error.as_deref().map(summarize_peer_error_detail).unwrap_or_default();
    if matches!(bucket.status.as_str(), "completed" | "canceled" | "failed") {
        bucket.completed_at = bucket.updated_at;
    }
}

fn summarize_site_resync_status(status: &mut SRResyncOpStatus, now: OffsetDateTime) {
    status.total_buckets = status.buckets.len() as u64;
    status.pending_buckets = 0;
    status.running_buckets = 0;
    status.completed_buckets = 0;
    status.failed_buckets = 0;
    status.canceled_buckets = 0;
    status.replicated_objects = 0;
    status.replicated_bytes = 0;
    status.failed_objects = 0;
    status.failed_bytes = 0;

    for bucket in &status.buckets {
        match bucket.status.as_str() {
            "pending" => status.pending_buckets += 1,
            "running" | "started" => status.running_buckets += 1,
            "completed" | "success" => status.completed_buckets += 1,
            "canceled" => status.canceled_buckets += 1,
            _ => status.failed_buckets += 1,
        }
        status.replicated_objects = status.replicated_objects.saturating_add(bucket.replicated_objects);
        status.replicated_bytes = status.replicated_bytes.saturating_add(bucket.replicated_bytes);
        status.failed_objects = status.failed_objects.saturating_add(bucket.failed_objects);
        status.failed_bytes = status.failed_bytes.saturating_add(bucket.failed_bytes);
    }

    status.updated_at = Some(now);
    status.status = if status.failed_buckets > 0 { "failed" } else { "success" }.to_string();
    let has_active_buckets = status.pending_buckets > 0
        || status.running_buckets > 0
        || status.buckets.iter().any(|bucket| bucket.status == "conflict");
    status.state = if has_active_buckets {
        if status.op_type == SITE_REPL_RESYNC_CANCEL {
            "canceling"
        } else if status.running_buckets > 0 {
            "running"
        } else {
            "pending"
        }
    } else if status.failed_buckets > 0 {
        "failed"
    } else if status.op_type == SITE_REPL_RESYNC_CANCEL || status.canceled_buckets == status.total_buckets {
        "canceled"
    } else {
        "completed"
    }
    .to_string();
    if matches!(status.state.as_str(), "completed" | "canceled" | "failed") && status.completed_at.is_none() {
        status.completed_at = Some(now);
    }
    status.err_detail = if status.failed_buckets > 0 {
        format!("{} of {} buckets failed", status.failed_buckets, status.total_buckets)
    } else {
        String::new()
    };
}

fn site_resync_page(status: &SRResyncOpStatus, limit: usize, offset: usize) -> S3Result<SRResyncOpStatus> {
    if offset > status.buckets.len() {
        return Err(s3_error!(InvalidRequest, "invalid resync continuation token"));
    }
    let mut response = status.clone();
    let end = offset.saturating_add(limit).min(status.buckets.len());
    response.buckets = status.buckets[offset..end].to_vec();
    response.truncated = end < status.buckets.len();
    response.next_continuation_token = if response.truncated {
        let token = SiteResyncContinuationToken {
            id: status.resync_id.clone(),
            generation: status.generation,
            offset: end,
        };
        let encoded = serde_json::to_vec(&token)
            .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("encode resync cursor failed: {err}")))?;
        URL_SAFE_NO_PAD.encode(encoded)
    } else {
        String::new()
    };
    Ok(response)
}

fn parse_site_resync_page(query: &HashMap<String, String>, status: &SRResyncOpStatus) -> S3Result<(usize, usize)> {
    let limit = query
        .get("limit")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| s3_error!(InvalidRequest, "invalid resync page limit"))?
        .unwrap_or(SITE_REPL_RESYNC_DEFAULT_PAGE_SIZE);
    if limit == 0 || limit > SITE_REPL_RESYNC_MAX_PAGE_SIZE {
        return Err(s3_error!(InvalidRequest, "invalid resync page limit"));
    }
    let offset = if let Some(value) = query.get("continuationToken") {
        let decoded = URL_SAFE_NO_PAD
            .decode(value)
            .map_err(|_| s3_error!(InvalidRequest, "invalid resync continuation token"))?;
        let token: SiteResyncContinuationToken =
            serde_json::from_slice(&decoded).map_err(|_| s3_error!(InvalidRequest, "invalid resync continuation token"))?;
        if token.id != status.resync_id || token.generation != status.generation {
            return Err(s3_error!(InvalidRequest, "stale resync continuation token"));
        }
        token.offset
    } else {
        0
    };
    Ok((limit, offset))
}

fn bucket_target_endpoint(target: &BucketTarget) -> String {
    let scheme = if target.secure { "https" } else { "http" };
    canonical_endpoint(&format!("{scheme}://{}", target.endpoint))
}

fn bucket_target_matches_peer(target: &BucketTarget, peer: &PeerInfo) -> bool {
    if !target.deployment_id.is_empty() {
        return target.deployment_id == peer.deployment_id;
    }
    bucket_target_endpoint(target) == canonical_endpoint(&peer.endpoint)
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
        if let Some(deployment_id) = replication_target_arn_deployment_id(&arn) {
            arns_by_peer.entry(deployment_id).or_insert(arn);
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
    let region = current_region()
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
        skip_tls_verify: peer.skip_tls_verify,
        ca_cert_pem: peer.ca_cert_pem.clone(),
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
    let parts: Vec<_> = arn.split(':').collect();
    if parts.len() == 6
        && parts[0] == "arn"
        && matches!(parts[1], "rustfs" | "minio")
        && parts[2] == "replication"
        && !parts[4].is_empty()
    {
        return Some(parts[4].to_string());
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

fn is_site_replication_rule(rule: &ReplicationRule) -> bool {
    rule.id.as_deref().is_some_and(|id| id.starts_with("site-repl-"))
}

/// Whether every `site-repl-*` rule on this bucket resolves to a live remote target.
///
/// The rule set alone cannot answer this: a rule can be perfectly formed while the endpoint
/// recorded for its peer is one this site cannot reach, so `update_all_targets` never built
/// a client for it and `replicate_object` drops every object against that ARN. Reads the
/// already-resolved client map rather than rebuilding clients, so it stays cheap enough for
/// the status path.
async fn site_replication_targets_online(bucket: &str, replication_config_xml: &[u8]) -> bool {
    let Ok(config) = deserialize::<ReplicationConfiguration>(replication_config_xml) else {
        return true;
    };

    for rule in config.rules.iter().filter(|rule| is_site_replication_rule(rule)) {
        if BucketTargetSys::get()
            .get_remote_target_client_by_arn(bucket, &rule.destination.bucket)
            .await
            .is_none()
        {
            return false;
        }
    }

    true
}

/// Merge a peer's replication config into the local one.
///
/// `site-repl-*` rules encode the *sender's* outbound direction — their destination ARN
/// names the receiver — so applying a peer's rule set verbatim replaces the receiver's
/// reverse rule with one pointing at itself. No bucket target can satisfy that ARN
/// (`reconcile_site_replication_bucket_targets` skips the local peer), so the receiver
/// silently stops replicating back: the one-directional symptom. Only operator-authored
/// rules travel between sites; each site owns its own `site-repl-*` rules.
fn merge_incoming_replication_config(
    incoming: Option<ReplicationConfiguration>,
    local: Option<ReplicationConfiguration>,
) -> Option<ReplicationConfiguration> {
    let incoming_role = incoming.as_ref().map(|config| config.role.clone()).unwrap_or_default();
    // Operator rules first, then the local site rules — the same order
    // `ensure_site_replication_bucket_replication_config_with_runtime` produces, so its
    // no-op check matches and the bucket metadata is written once per broadcast, not twice.
    let mut rules: Vec<ReplicationRule> = incoming
        .into_iter()
        .flat_map(|config| config.rules)
        .filter(|rule| !is_site_replication_rule(rule))
        .collect();
    rules.extend(
        local
            .into_iter()
            .flat_map(|config| config.rules)
            .filter(is_site_replication_rule),
    );

    if rules.is_empty() {
        return None;
    }

    for (index, rule) in rules.iter_mut().enumerate() {
        rule.priority = Some(i32::try_from(index + 1).unwrap_or(i32::MAX));
    }

    // A site-replication ARN in `role` is the sender's, and `site_replication_target_arns_by_peer`
    // reads it — carrying it over would pin the receiver's targets to the sender's identity.
    let role = match replication_target_arn_deployment_id(&incoming_role) {
        Some(_) => String::new(),
        None => incoming_role,
    };

    Some(ReplicationConfiguration { role, rules })
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
    existing: Option<&ReplicationConfiguration>,
) -> S3Result<Option<ReplicationConfiguration>> {
    // Reuse the ARN already recorded for a peer so the rule keeps pointing at the same
    // bucket target `reconcile_site_replication_bucket_targets` keys off (a MinIO-era
    // `arn:minio:...` target would otherwise be orphaned by a freshly minted ARN).
    let configured_arns = site_replication_target_arns_by_peer(existing);
    let mut rules = Vec::new();
    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }

        let Some(target) = site_replication_bucket_target_for_peer(
            bucket,
            state,
            peer,
            service_account_secret_key,
            configured_arns.get(&peer.deployment_id).cloned(),
        )?
        else {
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

async fn ensure_site_replication_bucket_targets_with_runtime(
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    config: Option<&s3s::dto::ReplicationConfiguration>,
    service_account_secret_key: &str,
    expected_incarnation_id: Uuid,
) -> S3Result<()> {
    let existing = match metadata_sys::list_bucket_targets(bucket).await {
        Ok(targets) => targets,
        Err(StorageError::ConfigNotFound) => BucketTargets::default(),
        Err(err) => return Err(ApiError::from(err).into()),
    };
    let existing_json = serde_json::to_vec(&existing)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize bucket targets failed: {e}")))?;

    let updated =
        reconcile_site_replication_bucket_targets(existing, bucket, state, local_peer, config, service_account_secret_key)?;
    if updated.targets.is_empty() {
        return Ok(());
    }

    let json_targets = serde_json::to_vec(&updated)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize bucket targets failed: {e}")))?;
    // Rewriting identical targets would churn bucket metadata and rebuild every remote S3
    // client — noticeable now that startup reconciles all buckets, not just the one bucket
    // an operation touched.
    if json_targets == existing_json {
        return Ok(());
    }
    metadata_sys::update_if_incarnation(bucket, BUCKET_TARGETS_FILE, json_targets, expected_incarnation_id)
        .await
        .map_err(ApiError::from)?;
    Ok(())
}

async fn bucket_replication_config_for_target_refresh(bucket: &str) -> S3Result<Option<s3s::dto::ReplicationConfiguration>> {
    match metadata_sys::get_replication_config(bucket).await {
        Ok((config, _)) => Ok(Some(config)),
        Err(StorageError::ConfigNotFound) => Ok(None),
        Err(err) => Err(ApiError::from(err).into()),
    }
}

async fn ensure_site_replication_bucket_targets(bucket: &str) -> S3Result<()> {
    let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(bucket)
        .await
        .map_err(ApiError::from)?;
    let _targets_guard = lock_bucket_targets_metadata(bucket).await;
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(());
    };
    let config = bucket_replication_config_for_target_refresh(bucket).await?;
    ensure_site_replication_bucket_targets_with_runtime(
        bucket,
        &runtime.state,
        &runtime.local_peer,
        config.as_ref(),
        &runtime.service_account_secret_key,
        expected_incarnation_id,
    )
    .await
}

async fn ensure_site_replication_bucket_replication_config_with_runtime(
    bucket: &str,
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    service_account_secret_key: &str,
    expected_incarnation_id: Uuid,
) -> S3Result<()> {
    let existing = match metadata_sys::get_replication_config(bucket).await {
        Ok((existing, _)) => Some(existing),
        Err(StorageError::ConfigNotFound) => None,
        Err(err) => return Err(ApiError::from(err).into()),
    };

    let Some(desired) = build_site_replication_config(bucket, state, local_peer, service_account_secret_key, existing.as_ref())?
    else {
        return Ok(());
    };

    // `site-repl-*` rules are derived state owned by this site: rebuild them from the
    // current peer set on every pass instead of preserving whatever is on disk. A rule
    // left over from a removed peer — or one whose destination ARN names this very
    // deployment, which no bucket target can ever satisfy — must not survive, otherwise
    // objects are queued against an ARN that resolves to nothing.
    let (existing_role, existing_rules) = existing
        .map(|config| (config.role, config.rules))
        .unwrap_or_else(|| (String::new(), Vec::new()));
    let mut rules: Vec<ReplicationRule> = existing_rules
        .iter()
        .filter(|rule| !is_site_replication_rule(rule))
        .cloned()
        .collect();
    rules.extend(desired.rules);
    for (index, rule) in rules.iter_mut().enumerate() {
        rule.priority = Some(i32::try_from(index + 1).unwrap_or(i32::MAX));
    }

    // Only a site-replication ARN in `role` is ours to drop — an operator-authored role is
    // part of the bucket's S3-visible configuration, and repairing a reverse rule must not
    // quietly rewrite it. Same rule as `merge_incoming_replication_config`.
    let role = match replication_target_arn_deployment_id(&existing_role) {
        Some(_) => String::new(),
        None => existing_role.clone(),
    };

    if rules == existing_rules && role == existing_role {
        return Ok(());
    }

    let data = serialize(&ReplicationConfiguration { role, rules })
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("serialize replication failed: {e}")))?;
    metadata_sys::update_if_incarnation(bucket, BUCKET_REPLICATION_CONFIG, data, expected_incarnation_id)
        .await
        .map_err(ApiError::from)?;

    Ok(())
}

async fn ensure_site_replication_bucket_setup(bucket: &str) -> S3Result<bool> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(false);
    };
    let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(bucket)
        .await
        .map_err(ApiError::from)?;
    ensure_site_replication_bucket_setup_with_runtime_for_incarnation(bucket, &runtime, expected_incarnation_id).await?;
    Ok(true)
}

async fn ensure_site_replication_bucket_setup_for_incarnation(bucket: &str, incarnation_id: Uuid) -> S3Result<bool> {
    let Some(runtime) = runtime_site_replication_targets().await? else {
        return Ok(false);
    };
    ensure_site_replication_bucket_setup_with_runtime_for_incarnation(bucket, &runtime, incarnation_id).await?;
    Ok(true)
}

async fn ensure_site_replication_bucket_setup_with_runtime(bucket: &str, runtime: &SiteReplicationRuntime) -> S3Result<()> {
    let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(bucket)
        .await
        .map_err(ApiError::from)?;
    ensure_site_replication_bucket_setup_with_runtime_for_incarnation(bucket, runtime, expected_incarnation_id).await
}

async fn ensure_site_replication_bucket_setup_with_runtime_for_incarnation(
    bucket: &str,
    runtime: &SiteReplicationRuntime,
    expected_incarnation_id: Uuid,
) -> S3Result<()> {
    let _targets_guard = lock_bucket_targets_metadata(bucket).await;
    let config = bucket_replication_config_for_target_refresh(bucket).await?;
    ensure_site_replication_bucket_targets_with_runtime(
        bucket,
        &runtime.state,
        &runtime.local_peer,
        config.as_ref(),
        &runtime.service_account_secret_key,
        expected_incarnation_id,
    )
    .await?;
    ensure_site_replication_bucket_replication_config_with_runtime(
        bucket,
        &runtime.state,
        &runtime.local_peer,
        &runtime.service_account_secret_key,
        expected_incarnation_id,
    )
    .await?;
    Ok(())
}

async fn cleanup_removed_site_replication_bucket(bucket: &str, removed_deployment_ids: &HashSet<String>) -> S3Result<usize> {
    let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(bucket)
        .await
        .map_err(ApiError::from)?;
    let _targets_guard = lock_bucket_targets_metadata(bucket).await;
    let mut removed = 0usize;

    match metadata_sys::list_bucket_targets(bucket).await {
        Ok(targets) => {
            let (updated_targets, removed_targets) =
                prune_removed_site_replication_bucket_targets(targets, removed_deployment_ids);
            if removed_targets > 0 {
                let json_targets = serde_json::to_vec(&updated_targets).map_err(|e| {
                    S3Error::with_message(S3ErrorCode::InternalError, format!("serialize bucket targets failed: {e}"))
                })?;
                metadata_sys::update_if_incarnation(bucket, BUCKET_TARGETS_FILE, json_targets, expected_incarnation_id)
                    .await
                    .map_err(ApiError::from)?;
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
                    metadata_sys::update_if_incarnation(bucket, BUCKET_REPLICATION_CONFIG, data, expected_incarnation_id)
                        .await
                        .map_err(ApiError::from)?;
                } else {
                    metadata_sys::delete_if_incarnation(bucket, BUCKET_REPLICATION_CONFIG, expected_incarnation_id)
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

    let Some(store) = current_object_store_handle() else {
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
/// kick a resync toward every remote peer so pre-existing objects back-fill. Returns a list of
/// human-readable per-bucket failure messages (empty on full success) so the caller can surface
/// them to the operator instead of silently reporting success; a failure never aborts the caller.
/// Probe every remote peer from the joining site before reporting the join a success.
///
/// A peer's endpoint is whatever that peer derived from the `Host` header of the admin
/// request that created the topology, so the initiator can record an address only it can
/// reach — a console-port rewrite, a NAT address, a LAN-only host. The initiator's own
/// probes all succeed in that case, and the reverse direction then fails silently forever
/// because nothing else pushes from here until an object is written. Report it in the add
/// response instead of rejecting the join: an operator may legitimately be opening the
/// return path afterwards.
async fn probe_reverse_peer_reachability(state: &SiteReplicationState, local_peer: &PeerInfo) -> SiteReplicationErrorSummary {
    let mut errors = SiteReplicationErrorSummary::default();
    let secret_key = match site_replicator_service_account_secret(&state.service_account_access_key).await {
        Ok(secret) => secret,
        Err(err) => {
            errors.push(format!("reverse reachability probe skipped: {err}"));
            return errors;
        }
    };

    for peer in state.peers.values() {
        if peer.deployment_id == local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
            continue;
        }
        let connection = match runtime_peer_connection(peer) {
            Ok(connection) => connection,
            Err(err) => {
                errors.push(format!("{} is not reachable from this site: {err}", peer.endpoint));
                continue;
            }
        };
        if let Err(err) = send_peer_admin_request(
            &connection,
            SITE_REPLICATION_DEVNULL_PATH,
            &state.service_account_access_key,
            &secret_key,
            &serde_json::json!({}),
        )
        .await
        {
            errors.push(format!("{} is not reachable from this site: {err}", peer.endpoint));
        }
    }

    errors
}

async fn backfill_existing_buckets_after_add(
    state: &SiteReplicationState,
    local_peer: &PeerInfo,
    bootstrap_token: Option<&str>,
) -> SiteReplicationErrorSummary {
    let mut errors = SiteReplicationErrorSummary::default();
    let Some(store) = current_object_store_handle() else {
        errors.push("object store not initialized; pre-existing buckets were not backfilled");
        return errors;
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
            errors.push(format!("list buckets failed: {err}"));
            return errors;
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
            errors.push(format!("{name}: versioning setup failed: {err}"));
            continue;
        }
        match ensure_site_replication_bucket_setup(name).await {
            Ok(true) => {}
            Ok(false) => {
                // Runtime targets unavailable: the setup silently no-ops, which would make the
                // downstream make-bucket broadcast and resync fail. Record it and skip so the
                // operator sees this bucket was not propagated instead of an unqualified success.
                warn!(
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    bucket = %name,
                    result = "backfill_bucket_setup_skipped",
                    "admin site replication state"
                );
                errors.push(format!("{name}: replication setup skipped (site replication runtime unavailable)"));
                continue;
            }
            Err(err) => {
                warn!(
                    event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                    component = LOG_COMPONENT_ADMIN,
                    subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                    bucket = %name,
                    result = "backfill_bucket_setup_failed",
                    error = ?err,
                    "admin site replication state"
                );
                errors.push(format!("{name}: bucket setup failed: {err}"));
            }
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
        if let Err(err) = broadcast_site_replication_make_bucket(name, lock_enabled, None, bootstrap_token).await {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                bucket = %name,
                result = "backfill_make_bucket_broadcast_failed",
                error = ?err,
                "admin site replication state"
            );
            errors.push(format!("{name}: make-bucket broadcast failed: {err}"));
        }
        // Kick a resync toward every remote peer so existing objects travel across.
        for peer in state.peers.values() {
            if peer.deployment_id == local_peer.deployment_id || same_identity_endpoint(&peer.endpoint, &local_peer.endpoint) {
                continue;
            }
            let manifest = site_bucket_resync_manifest_entry(name, peer, OffsetDateTime::now_utc()).await;
            let result = if manifest.target_arn.is_empty() {
                manifest
            } else {
                start_site_bucket_resync(name, &manifest.target_arn, &resync_id).await
            };
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
                errors.push(format!("{name} -> {}: resync kick failed: {}", peer.endpoint, result.err_detail));
            }
        }
    }
    errors
}

async fn refresh_bucket_targets_after_service_account_rotation() {
    let Some(store) = current_object_store_handle() else {
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
        if let Err(err) = ensure_site_replication_bucket_targets(&bucket.name).await {
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

async fn refresh_bucket_targets_after_endpoint_edit(pending_id: &str, service_account_secret_key: &str) -> S3Result<()> {
    let store = current_object_store_handle()
        .ok_or_else(|| S3Error::with_message(S3ErrorCode::InternalError, "object store is not initialized".to_string()))?;
    let buckets = store.list_bucket(&BucketOptions::default()).await.map_err(ApiError::from)?;

    for bucket in buckets {
        let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(&bucket.name)
            .await
            .map_err(ApiError::from)?;
        // Read-only per bucket: the pending refresh is re-read (and re-checked)
        // every round, and the writes below are bucket metadata, not state.
        let state = load_site_replication_state().await?;
        let Some(pending) = pending_endpoint_refresh(&state).filter(|pending| pending.id == pending_id) else {
            return Err(s3_error!(InvalidRequest, "endpoint target refresh state changed during update"));
        };
        let target_state = endpoint_refresh_target_state(&state, &pending);
        let local_peer = current_local_runtime_peer(&target_state);
        let _targets_guard = lock_bucket_targets_metadata(&bucket.name).await;
        let replication_config = bucket_replication_config_for_target_refresh(&bucket.name).await?;
        ensure_site_replication_bucket_targets_with_runtime(
            &bucket.name,
            &target_state,
            &local_peer,
            replication_config.as_ref(),
            service_account_secret_key,
            expected_incarnation_id,
        )
        .await?;
    }

    Ok(())
}

async fn site_bucket_resync_manifest_entry(bucket: &str, peer: &PeerInfo, now: OffsetDateTime) -> ResyncBucketStatus {
    let mut entry = ResyncBucketStatus {
        bucket: bucket.to_string(),
        status: "pending".to_string(),
        created_at: Some(now),
        updated_at: Some(now),
        ..Default::default()
    };
    let _targets_guard = lock_bucket_targets_metadata(bucket).await;
    let (config, _) = match metadata_sys::get_replication_config(bucket).await {
        Ok(config) => config,
        Err(err) => {
            entry.status = "failed".to_string();
            entry.err_detail = summarize_peer_error_detail(&err.to_string());
            return entry;
        }
    };
    let targets = match metadata_sys::list_bucket_targets(bucket).await {
        Ok(targets) => targets,
        Err(err) => {
            entry.status = "failed".to_string();
            entry.err_detail = summarize_peer_error_detail(&err.to_string());
            return entry;
        }
    };
    let mut matching = targets
        .targets
        .iter()
        .filter(|target| target.target_type == BucketTargetType::ReplicationService && bucket_target_matches_peer(target, peer));
    let Some(target) = matching.next() else {
        entry.status = "failed".to_string();
        entry.err_detail = "no valid remote target found for peer".to_string();
        return entry;
    };
    if matching.next().is_some() {
        entry.status = "failed".to_string();
        entry.err_detail = "multiple remote targets matched peer".to_string();
        return entry;
    }
    let (has_arn, existing_object_enabled) = config.has_existing_object_replication(&target.arn);
    if !has_arn || !existing_object_enabled {
        entry.status = "failed".to_string();
        entry.err_detail = "existing object replication is not enabled for the peer target".to_string();
        return entry;
    }
    entry.target_arn = target.arn.clone();
    entry
}

async fn start_site_bucket_resync(bucket: &str, target_arn: &str, resync_id: &str) -> ResyncBucketStatus {
    let mut bucket_status = ResyncBucketStatus {
        bucket: bucket.to_string(),
        target_arn: target_arn.to_string(),
        status: "running".to_string(),
        ..Default::default()
    };
    let Some(pool) = current_replication_pool_handle() else {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = "replication pool is not initialized".to_string();
        return bucket_status;
    };
    let _targets_guard = lock_bucket_targets_metadata(bucket).await;
    let transaction_guard = match metadata_sys::acquire_bucket_metadata_transaction_lock(bucket).await {
        Ok(guard) => guard,
        Err(_) => {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = "replication target metadata transaction lock is unavailable".to_string();
            return bucket_status;
        }
    };

    let (config, _) = match metadata_sys::get_replication_config(bucket).await {
        Ok(config) => config,
        Err(err) => {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = err.to_string();
            return bucket_status;
        }
    };

    let targets = match metadata_sys::list_bucket_targets_from_disk(bucket).await {
        Ok(targets) => targets,
        Err(err) => {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = err.to_string();
            return bucket_status;
        }
    };
    let Some(target_index) = targets
        .targets
        .iter()
        .position(|target| target.target_type == BucketTargetType::ReplicationService && target.arn == target_arn)
    else {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = "recorded remote target no longer exists".to_string();
        return bucket_status;
    };

    let existing_reset_id = targets.targets[target_index].reset_id.clone();
    if !existing_reset_id.is_empty() && existing_reset_id != resync_id {
        let existing_is_active = pool
            .get_bucket_resync_status(bucket)
            .await
            .ok()
            .and_then(|status| status.targets_map.get(target_arn).cloned())
            .is_none_or(|target| target.resync_id != existing_reset_id || site_bucket_resync_is_active(target.resync_status));
        if existing_is_active {
            bucket_status.status = "conflict".to_string();
            bucket_status.err_detail = "target belongs to a different active resync operation".to_string();
            return bucket_status;
        }
    }

    let reset_before = Some(OffsetDateTime::now_utc());
    let target_arn = {
        let target = &targets.targets[target_index];

        let (has_arn, existing_object_enabled) = config.has_existing_object_replication(&target.arn);
        if !has_arn || !existing_object_enabled {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = "existing object replication is not enabled for the peer target".to_string();
            return bucket_status;
        }

        target.arn.clone()
    };

    let opts = replication::resync_opts(bucket, target_arn.clone(), resync_id, reset_before);
    let admission_pool = pool.clone();
    let activation_pool = pool.clone();
    let _committed_targets = match replication::commit_resync_target(
        targets,
        opts,
        move |opts| async move { admission_pool.admit_bucket_resync(opts).await },
        move |encoded| async move {
            metadata_sys::update_bucket_targets_under_transaction_lock(&transaction_guard, bucket, encoded)
                .await
                .map(|_| ())
                .map_err(|_| {
                    StorageError::other(
                        "replication resync was accepted but target metadata commit failed; retry the same resync ID to reconcile",
                    )
                })
        },
        move |opts, recovering| async move { activation_pool.activate_bucket_resync(opts, recovering).await },
    )
    .await
    {
        Ok(targets) => targets,
        Err(err) => {
            bucket_status.status = "failed".to_string();
            if let Some(active_resync_id) = replication::resync_start_conflict_id(&err) {
                bucket_status.status = "conflict".to_string();
                bucket_status.err_detail =
                    format!("replication resync {active_resync_id} is already active for this target");
            } else {
                bucket_status.err_detail = err.to_string();
            }
            return bucket_status;
        }
    };
    bucket_status
}

async fn cancel_site_bucket_resync(bucket: &str, target_arn: &str, resync_id: &str) -> ResyncBucketStatus {
    let mut bucket_status = ResyncBucketStatus {
        bucket: bucket.to_string(),
        target_arn: target_arn.to_string(),
        status: "canceled".to_string(),
        ..Default::default()
    };
    let expected_incarnation_id = match metadata_sys::capture_bucket_metadata_incarnation(bucket).await {
        Ok(incarnation_id) => incarnation_id,
        Err(err) => {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = err.to_string();
            return bucket_status;
        }
    };
    let targets_guard = lock_bucket_targets_metadata(bucket).await;

    let mut targets = match metadata_sys::list_bucket_targets(bucket).await {
        Ok(targets) => targets,
        Err(err) => {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = err.to_string();
            return bucket_status;
        }
    };

    let Some(target) = targets.targets.iter_mut().find(|target| {
        target.target_type == BucketTargetType::ReplicationService && target.arn == target_arn && target.reset_id == resync_id
    }) else {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = "recorded resync target is not in progress".to_string();
        return bucket_status;
    };

    let target_arn = target.arn.clone();

    let Some(pool) = current_replication_pool_handle() else {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = "replication pool is not initialized".to_string();
        return bucket_status;
    };

    if let Err(err) = pool
        .cancel_bucket_resync(replication::resync_opts(bucket, target_arn, resync_id, None))
        .await
    {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = err.to_string();
        return bucket_status;
    }

    target.reset_id.clear();
    target.reset_before_date = None;

    let json_targets = match serde_json::to_vec(&targets) {
        Ok(json_targets) => json_targets,
        Err(err) => {
            bucket_status.status = "failed".to_string();
            bucket_status.err_detail = err.to_string();
            return bucket_status;
        }
    };

    if let Err(err) =
        metadata_sys::update_if_incarnation(bucket, BUCKET_TARGETS_FILE, json_targets, expected_incarnation_id).await
    {
        bucket_status.status = "failed".to_string();
        bucket_status.err_detail = err.to_string();
        return bucket_status;
    }
    drop(targets_guard);

    bucket_status
}

async fn refresh_site_resync_status(mut status: SRResyncOpStatus, peer: &PeerInfo) -> SRResyncOpStatus {
    for bucket in &mut status.buckets {
        if bucket.target_arn.is_empty() && matches!(bucket.status.as_str(), "pending" | "running" | "started") {
            let resolved = site_bucket_resync_manifest_entry(&bucket.bucket, peer, OffsetDateTime::now_utc()).await;
            if resolved.target_arn.is_empty() {
                bucket.status = "failed".to_string();
                bucket.err_detail = resolved.err_detail;
            } else {
                bucket.target_arn = resolved.target_arn;
                bucket.status = "pending".to_string();
            }
        }
    }
    if let Some(pool) = current_replication_pool_handle() {
        for bucket in &mut status.buckets {
            if bucket.target_arn.is_empty() || bucket.status == "failed" {
                continue;
            }
            match pool.get_bucket_resync_status(&bucket.bucket).await {
                Ok(live) => match live.targets_map.get(&bucket.target_arn) {
                    Some(target) if target.resync_id == status.resync_id => {
                        apply_site_resync_target_status(bucket, target);
                    }
                    Some(target) if !target.resync_id.is_empty() && site_bucket_resync_is_active(target.resync_status) => {
                        bucket.status = "conflict".to_string();
                        bucket.err_detail = "recorded target belongs to a different resync operation".to_string();
                        bucket.updated_at = Some(OffsetDateTime::now_utc());
                    }
                    Some(target) if !target.resync_id.is_empty() => {
                        bucket.status = "failed".to_string();
                        bucket.err_detail = "recorded resync operation was superseded by a terminal bucket resync".to_string();
                        bucket.updated_at = Some(OffsetDateTime::now_utc());
                        bucket.completed_at = bucket.updated_at;
                    }
                    _ if matches!(bucket.status.as_str(), "pending" | "running" | "started") => {
                        let previous = bucket.clone();
                        let mut recovered =
                            start_site_bucket_resync(&previous.bucket, &previous.target_arn, &status.resync_id).await;
                        recovered.created_at = previous.created_at;
                        recovered.started_at = previous.started_at.or(Some(OffsetDateTime::now_utc()));
                        recovered.updated_at = Some(OffsetDateTime::now_utc());
                        recovered.generation = status.generation;
                        recovered.err_detail = summarize_peer_error_detail(&recovered.err_detail);
                        *bucket = recovered;
                    }
                    _ => {}
                },
                Err(err) => {
                    bucket.err_detail = summarize_peer_error_detail(&err.to_string());
                    bucket.updated_at = Some(OffsetDateTime::now_utc());
                }
            }
        }
    }
    summarize_site_resync_status(&mut status, OffsetDateTime::now_utc());
    status
}

async fn persist_site_resync_status(peer_id: &str, status: &SRResyncOpStatus) -> S3Result<()> {
    let peer_id = peer_id.to_string();
    let status = status.clone();
    update_site_replication_state(move |state| {
        // The run identity is checked inside the transaction: a cancel or a
        // newer run that committed while this progress snapshot was being
        // built must not be overwritten by it.
        if state
            .resync_status
            .get(&peer_id)
            .is_some_and(|current| current.resync_id != status.resync_id || current.generation != status.generation)
        {
            return Err(s3_error!(InvalidRequest, "site replication resync state changed"));
        }
        state.resync_status.insert(peer_id, status);
        Ok(())
    })
    .await
}

async fn persist_new_site_resync_status(peer_id: &str, status: &SRResyncOpStatus) -> S3Result<()> {
    let peer_id = peer_id.to_string();
    let status = status.clone();
    update_site_replication_state(move |state| {
        if state.resync_status.get(&peer_id).is_some_and(site_resync_is_active) {
            return Err(s3_error!(InvalidRequest, "site replication resync is already active"));
        }
        state.resync_status.insert(peer_id, status);
        Ok(())
    })
    .await
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
    let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(bucket)
        .await
        .map_err(ApiError::from)?;
    match metadata_sys::get_versioning_config(bucket).await {
        Ok((config, _)) if config.enabled() => return Ok(()),
        Ok(_) | Err(StorageError::ConfigNotFound) => {}
        Err(err) => return Err(ApiError::from(err).into()),
    }

    metadata_sys::update_if_incarnation(bucket, BUCKET_VERSIONING_CONFIG, bucket_versioning_xml()?, expected_incarnation_id)
        .await
        .map_err(ApiError::from)?;

    Ok(())
}

fn is_stale_update(local_updated_at: OffsetDateTime, incoming_updated_at: Option<OffsetDateTime>) -> bool {
    incoming_updated_at.is_some_and(|incoming_updated_at| incoming_updated_at < local_updated_at)
}

fn bucket_meta_local_updated_at(
    bucket_meta: &crate::admin::storage_api::bucket::metadata::BucketMetadata,
    config_file: &str,
) -> OffsetDateTime {
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
    let Some(store) = current_object_store_handle() else {
        return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
    };
    let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(&item.bucket)
        .await
        .map_err(ApiError::from)?;

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
    let targets_guard = if item.r#type == "replication-config" {
        Some(lock_bucket_targets_metadata(&item.bucket).await)
    } else {
        None
    };
    if let Ok(bucket_meta) = metadata_sys::get(&item.bucket).await {
        let local_updated_at = bucket_meta_local_updated_at(&bucket_meta, config_file);
        if is_stale_update(local_updated_at, incoming_updated_at) {
            return Ok(());
        }
    }

    // Nothing to write and nothing on disk to clear: the common case for a site joining a
    // replicated bucket, where every incoming rule is the sender's own. Skipping the write
    // avoids stamping an empty config over a bucket that never had one;
    // `ensure_site_replication_bucket_setup` below still installs this site's own rules.
    let mut skip_config_write = false;
    let merged_replication_config =
        if item.r#type == "replication-config" {
            let incoming = item
                .replication_config
                .as_ref()
                .map(|raw| {
                    let data = decode_bucket_meta_wire_value(raw);
                    deserialize::<ReplicationConfiguration>(&data)
                })
                .transpose()
                .map_err(|e| s3_error!(InvalidRequest, "invalid replication config: {e}"))?;
            let local = match metadata_sys::get_replication_config(&item.bucket).await {
                Ok((config, _)) => Some(config),
                Err(StorageError::ConfigNotFound) => None,
                Err(err) => return Err(ApiError::from(err).into()),
            };
            let local_absent = local.is_none();
            match merge_incoming_replication_config(incoming, local) {
                Some(config) => Some(serialize(&config).map_err(|e| {
                    S3Error::with_message(S3ErrorCode::InternalError, format!("serialize replication failed: {e}"))
                })?),
                None => {
                    skip_config_write = local_absent;
                    None
                }
            }
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
        "replication-config" => merged_replication_config,
        "lc-config" => decode_bucket_meta_wire_option(item.expiry_lc_config),
        "cors-config" => decode_bucket_meta_wire_option(item.cors),
        _ => unreachable!(),
    };

    if !skip_config_write {
        if let Some(data) = data {
            metadata_sys::update_if_incarnation(&item.bucket, config_file, data, expected_incarnation_id)
                .await
                .map_err(ApiError::from)?;
        } else {
            metadata_sys::delete_if_incarnation(&item.bucket, config_file, expected_incarnation_id)
                .await
                .map_err(ApiError::from)?;
        }
    }
    drop(targets_guard);

    if item.r#type == "replication-config" {
        // Rebuild the local outbound rules too: a site that joined an already-replicated
        // bucket receives this item before it has any `site-repl-*` rule of its own.
        ensure_site_replication_bucket_setup_for_incarnation(&item.bucket, expected_incarnation_id).await?;
    }

    if item.r#type == "version-config"
        && metadata_sys::get_versioning_config(&item.bucket)
            .await
            .ok()
            .is_some_and(|(config, _)| config.enabled())
    {
        ensure_site_replication_bucket_setup_for_incarnation(&item.bucket, expected_incarnation_id).await?;
    }

    Ok(())
}

fn group_info_requires_upsert(update: &rustfs_madmin::GroupAddRemove) -> bool {
    !update.is_remove
}

pub(crate) fn encode_service_account_replication_policy(
    claims: &HashMap<String, Value>,
    session_policy: Option<&str>,
) -> S3Result<(SRSessionPolicy, Option<rustfs_madmin::SRSvcAccReplicationEnvelope>)> {
    if !claims.contains_key(OIDC_VIRTUAL_PARENT_CLAIM) {
        return session_policy
            .map(SRSessionPolicy::from_json)
            .transpose()
            .map(|policy| policy.unwrap_or_default())
            .map(|policy| (policy, None))
            .map_err(|err| s3_error!(InvalidArgument, "marshal policy failed: {:?}", err));
    }

    let policy = match session_policy {
        Some(policy) => serde_json::from_str::<Policy>(policy)
            .map_err(|err| s3_error!(InvalidArgument, "invalid service account replication policy: {:?}", err))?,
        None => Policy::default(),
    };
    if policy.statements.is_empty() && (!policy.id.is_empty() || !policy.version.is_empty())
        || policy.version.is_empty() && !policy.statements.is_empty()
    {
        return Err(s3_error!(InvalidArgument, "service account replication policy is not normalized"));
    }
    let policy = serde_json::to_string(&policy)
        .map_err(|err| s3_error!(InternalError, "marshal service account replication policy failed: {:?}", err))?;
    let policy = SRSessionPolicy::from_json(&policy)
        .map_err(|err| s3_error!(InternalError, "marshal service account replication policy failed: {:?}", err))?;
    Ok((
        policy,
        Some(rustfs_madmin::SRSvcAccReplicationEnvelope {
            version: SERVICE_ACCOUNT_ENVELOPE_VERSION,
        }),
    ))
}

#[derive(Debug)]
struct ReplicatedServiceAccountPolicy {
    policy: Option<Policy>,
    is_envelope: bool,
}

impl ReplicatedServiceAccountPolicy {
    fn for_existing_account(self) -> Option<Policy> {
        if self.is_envelope {
            Some(self.policy.unwrap_or_default())
        } else {
            self.policy
        }
    }

    fn metadata_for_existing_account(&self, value: String) -> Option<String> {
        (self.is_envelope || !value.is_empty()).then_some(value)
    }
}

fn decode_service_account_replication_policy(
    create: &SRSvcAccCreate,
    envelope: Option<&rustfs_madmin::SRSvcAccReplicationEnvelope>,
    incoming_updated_at: Option<OffsetDateTime>,
    local_updated_at: Option<OffsetDateTime>,
) -> S3Result<Option<ReplicatedServiceAccountPolicy>> {
    if local_updated_at.is_some_and(|local_updated_at| is_stale_update(local_updated_at, incoming_updated_at)) {
        return Ok(None);
    }

    let Some(envelope) = envelope else {
        return Ok(Some(ReplicatedServiceAccountPolicy {
            policy: create.session_policy.as_str().and_then(|raw| serde_json::from_str(raw).ok()),
            is_envelope: false,
        }));
    };
    if envelope.version != SERVICE_ACCOUNT_ENVELOPE_VERSION || !create.claims.contains_key(OIDC_VIRTUAL_PARENT_CLAIM) {
        return Err(s3_error!(InvalidRequest, "invalid service account replication envelope"));
    }

    if incoming_updated_at.is_none() {
        return Err(s3_error!(InvalidRequest, "service account replication envelope has no revision"));
    }
    let policy: Policy = serde_json::from_str(
        create
            .session_policy
            .as_str()
            .ok_or_else(|| s3_error!(InvalidRequest, "service account replication envelope has no session policy"))?,
    )
    .map_err(|err| s3_error!(InvalidRequest, "invalid replicated service account session policy: {}", err))?;
    if policy.statements.is_empty() && (!policy.id.is_empty() || !policy.version.is_empty())
        || policy.version.is_empty() && !policy.statements.is_empty()
    {
        return Err(s3_error!(InvalidRequest, "replicated service account policy is not normalized"));
    }
    let policy = (!policy.id.is_empty() || !policy.version.is_empty() || !policy.statements.is_empty()).then_some(policy);
    Ok(Some(ReplicatedServiceAccountPolicy {
        policy,
        is_envelope: true,
    }))
}

async fn apply_iam_item(item: SRIAMItem) -> S3Result<()> {
    let Some(iam_sys) = current_iam_handle() else {
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
            let user_type =
                user_type_from_sr_wire(mapping.user_type).ok_or_else(|| s3_error!(InvalidRequest, "invalid userType"))?;
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
        // MinIO madmin-go sends `SRIAMItemSTSAcc = "sts-account"`. The legacy alias
        // `sts-credential` (emitted by older RustFS releases) stays accepted permanently
        // so mixed-version RustFS sites keep replicating STS credentials during rolling
        // upgrades; it is a compatibility layer, not temporary code.
        SR_IAM_ITEM_STS_ACC | SR_IAM_ITEM_STS_ACC_LEGACY => {
            let Some(sts_credential) = item.sts_credential else {
                return Err(s3_error!(InvalidRequest, "stsCredential is required"));
            };
            let Some(secret) = current_token_signing_key() else {
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
            let groups = string_list_claim(&claims, "groups");
            let compatibility_policy = sts_replication_compatibility_policy(&claims, &sts_credential.parent_policy_mapping);
            let cred = rustfs_credentials::Credentials {
                access_key: sts_credential.access_key.clone(),
                secret_key: sts_credential.secret_key.clone(),
                session_token: sts_credential.session_token.clone(),
                expiration,
                status: "on".to_string(),
                parent_user: sts_credential.parent_user.clone(),
                groups,
                claims: Some(claims),
                ..Default::default()
            };
            iam_sys
                .set_temp_user(&sts_credential.access_key, &cred, compatibility_policy)
                .await
                .map_err(ApiError::from)?;
            Ok(())
        }
        "iam-user" => {
            let Some(user) = item.iam_user else {
                return Err(s3_error!(InvalidRequest, "iamUser is required"));
            };
            if let Some(local) = iam_sys.get_user(&user.access_key).await
                && is_stale_update(local.update_at.unwrap_or(OffsetDateTime::UNIX_EPOCH), incoming_updated_at)
            {
                return Ok(());
            }
            if user.is_delete_req {
                iam_sys.delete_user(&user.access_key, true).await.map_err(ApiError::from)?;
            } else {
                let Some(user_req) = user.user_req else {
                    return Err(s3_error!(InvalidRequest, "userReq is required"));
                };
                let is_status_only_update = user_req.secret_key.is_empty() && user_req.policy.is_none();
                if is_status_only_update {
                    iam_sys
                        .set_user_status(&user.access_key, user_req.status)
                        .await
                        .map_err(ApiError::from)?;
                } else {
                    iam_sys
                        .create_user(&user.access_key, &user_req)
                        .await
                        .map_err(ApiError::from)?;
                }
            }
            Ok(())
        }
        "service-account" => {
            let Some(change) = item.svc_acc_change else {
                return Err(s3_error!(InvalidRequest, "serviceAccountChange is required"));
            };
            let envelope = change.oidc_service_account_envelope;
            if let Some(create) = change.create {
                let local_updated_at = iam_sys
                    .get_user(&create.access_key)
                    .await
                    .map(|local| local.update_at.unwrap_or(OffsetDateTime::UNIX_EPOCH));
                let replicated_policy = if create.access_key == SITE_REPLICATOR_SERVICE_ACCOUNT {
                    if local_updated_at.is_some_and(|local_updated_at| is_stale_update(local_updated_at, incoming_updated_at)) {
                        return Ok(());
                    }
                    ReplicatedServiceAccountPolicy {
                        policy: Some(site_replicator_service_account_policy()?),
                        is_envelope: false,
                    }
                } else {
                    let Some(replicated_policy) = decode_service_account_replication_policy(
                        &create,
                        envelope.as_ref(),
                        incoming_updated_at,
                        local_updated_at,
                    )?
                    else {
                        return Ok(());
                    };
                    replicated_policy
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
                                    name: replicated_policy.metadata_for_existing_account(create.name),
                                    description: replicated_policy.metadata_for_existing_account(create.description),
                                    session_policy: replicated_policy.for_existing_account(),
                                    secret_key: Some(create.secret_key),
                                    expiration: create.expiration,
                                    status: (!create.status.is_empty()).then_some(create.status),
                                    parent_user: None,
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
                                    session_policy: replicated_policy.policy,
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
                            // Peers replicate credentials, never the local parent binding:
                            // each site resolves its own parent from its own IAM.
                            parent_user: None,
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

fn string_list_claim(claims: &HashMap<String, Value>, name: &str) -> Option<Vec<String>> {
    let values = claims.get(name)?.as_array()?;
    let values: Vec<String> = values
        .iter()
        .filter_map(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    (!values.is_empty()).then_some(values)
}

fn sts_replication_compatibility_policy<'a>(claims: &HashMap<String, Value>, parent_policy_mapping: &'a str) -> Option<&'a str> {
    (!claims.contains_key(OIDC_VIRTUAL_PARENT_CLAIM) && !parent_policy_mapping.is_empty()).then_some(parent_policy_mapping)
}

pub struct SiteReplicationAddHandler {}

/// MinIO's `SRPeerJoin` replies with an empty body on success; synthesize the
/// peer identity from the add preflight metainfo in that case.
fn parse_peer_join_response(body: &[u8], fallback_peer: PeerInfo) -> Result<SRPeerJoinResponse, serde_json::Error> {
    if body.iter().all(u8::is_ascii_whitespace) {
        return Ok(SRPeerJoinResponse {
            peer: fallback_peer,
            initial_sync_error_message: String::new(),
        });
    }
    serde_json::from_slice(body)
}

#[async_trait::async_trait]
impl Operation for SiteReplicationAddHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationAddAction).await?;
        reject_site_replicator_on_public_admin(&cred)?;
        let replicate_ilm_expiry = sr_add_replicate_ilm_expiry(&req.uri);
        let lifecycle_guard = SiteReplicationLifecycleGuard::acquire().await;
        // Everything up to the commit below is preflight: peer probes, IAM
        // work and the join fan-out all talk to the network, so none of it may
        // run inside the state transaction. The snapshot read here is what the
        // `updated_at` CAS in the commit validates.
        let current_state = load_site_replication_state().await?;
        if pending_endpoint_refresh(&current_state).is_some() {
            return Err(s3_error!(InvalidRequest, "endpoint target refresh is pending"));
        }
        let local_peer = current_local_peer(&req, &current_state);
        let mut sites: Vec<PeerSite> = read_site_replication_json(req, &cred.secret_key, true).await?;
        // The web console's "Set Up Site Replication" omits the local deployment from the payload;
        // inject it so the add preflight (which requires the local deployment) succeeds. No-op for `mc`.
        ensure_local_site_present(&mut sites, &local_peer);
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
        let expected_updated_at = current_state.updated_at;
        require_add_peer_tls_capability(&sites, &local_peer).await?;
        // Early exit on a state that moved under the preflight probes, BEFORE
        // the IAM write and the join fan-out change anything remote. Advisory
        // only — the binding check is the CAS inside the commit — but it fences
        // the common race off the side-effect path and refreshes the merge
        // base so the CAS window is only the join round trips.
        let latest_state = load_site_replication_state().await?;
        ensure_edit_precondition(&latest_state, expected_updated_at, None, "add preflight")?;
        let current_state = latest_state;
        let (service_account_access_key, service_account_secret_key) =
            ensure_site_replicator_service_account(&cred.access_key, false).await?;
        let bootstrap_buckets = preflight_infos
            .iter()
            .filter(|info| !same_identity_endpoint(&info.endpoint, &local_peer.endpoint))
            .flat_map(|info| info.bucket_names.iter().cloned())
            .collect();
        let add_in_progress_guard = SiteReplicationAddInProgressGuard::start(lifecycle_guard, bootstrap_buckets)?;
        let mut state = merge_add_sites(
            current_state,
            local_peer.clone(),
            sites.clone(),
            service_account_access_key.clone(),
            cred.access_key.clone(),
            replicate_ilm_expiry,
        );
        state.sync_state_initialized = true;
        let join_req = SRPeerJoinEnvelope {
            request: SRPeerJoinReq {
                svc_acct_access_key: service_account_access_key,
                svc_acct_secret_key: service_account_secret_key.clone(),
                svc_acct_parent: String::new(),
                peers: state.peers.clone(),
                updated_at: state.updated_at,
            },
            defer_sync_state_enable: true,
        };
        let peer_join_path =
            with_site_replication_bootstrap_token(SITE_REPLICATION_PEER_JOIN_PATH, &add_in_progress_guard.token.to_string());

        let mut joined_endpoints = HashSet::new();
        let mut initial_sync_errors = SiteReplicationErrorSummary::default();
        for (site, preflight) in sites.iter().zip(preflight_infos.iter()) {
            if same_identity_endpoint(&site.endpoint, &local_peer.endpoint)
                || !joined_endpoints.insert(site_identity_key(&site.endpoint))
            {
                continue;
            }

            let mut peer_join_req = join_req.clone();
            peer_join_req.request.svc_acct_parent = site.access_key.clone();
            let connection = PeerConnection::try_from(site)?;
            let body =
                send_peer_admin_request(&connection, &peer_join_path, &site.access_key, &site.secret_key, &peer_join_req).await?;

            let mut fallback_peer = existing_peer_for_endpoint(&state, &site.endpoint)
                .unwrap_or_else(|| normalize_peer_site(site.clone(), replicate_ilm_expiry));
            fallback_peer.deployment_id = preflight.deployment_id.clone();
            let join_response = parse_peer_join_response(&body, fallback_peer).map_err(|e| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("parse peer join response from {} failed: {e}", site.endpoint),
                )
            })?;
            if !join_response.initial_sync_error_message.is_empty() {
                initial_sync_errors.push(format!("{}: {}", site.endpoint, join_response.initial_sync_error_message));
            }
            state = reconcile_peer_with_actual_identity(state, join_response.peer);
            let reconciled_peer = existing_peer_for_endpoint(&state, &site.endpoint).ok_or_else(|| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    format!("peer join response from {} did not identify the requested site", site.endpoint),
                )
            })?;
            validate_proposed_peer(&reconciled_peer).map_err(|err| {
                S3Error::with_message(
                    S3ErrorCode::InvalidRequest,
                    format!("invalid peer join response from {}: {err}", site.endpoint),
                )
            })?;
        }

        mark_unknown_peer_sync_enabled(&mut state.peers);

        // Commit. The CAS runs inside the transaction, against the state the
        // transaction itself loaded — the peer round trips above took however
        // long they took, and only this check can tell whether the topology
        // this add was planned against is still the current one. The error
        // says so: by this point the remote sites already accepted their
        // joins, and re-running the add is what reconverges the local side.
        let next_state = state;
        let (state, edit_generation) = update_site_replication_state(move |state| {
            if state.updated_at != expected_updated_at || pending_endpoint_refresh(state).is_some() {
                return Err(s3_error!(
                    InvalidRequest,
                    "site replication state changed during peer join; the peers may already be joined — re-run replicate add"
                ));
            }
            // Adopt only the fields this add computed. Everything else is
            // owned by writers that commit without touching `updated_at`
            // (retry events, peer-edit generations, resync progress, the
            // acks/clears of an already pending rotation or removal), so the
            // CAS above cannot vouch for them — they keep the freshly loaded
            // value. The exhaustive destructure makes adding a state field a
            // compile error here until it is classified.
            let SiteReplicationState {
                name,
                service_account_access_key,
                service_account_secret_key: _,
                service_account_parent,
                peers,
                updated_at,
                resync_status: _,
                pending_rotation: _,
                pending_remove: _,
                pending_endpoint_refresh: _,
                retry_queue: _,
                sync_state_initialized,
                edit_generation: _,
                applied_edit_generations: _,
            } = next_state;
            state.name = name;
            state.service_account_access_key = service_account_access_key;
            state.service_account_parent = service_account_parent;
            state.peers = peers;
            state.updated_at = updated_at;
            state.sync_state_initialized = sync_state_initialized;
            let edit_generation = next_peer_edit_generation(state);
            Ok((state.clone(), edit_generation))
        })
        .await?;

        // The finalize fan-out delivers peer-edit payloads, so it carries the
        // generation allocated in the commit above: the receiving site orders
        // it against any edit that follows instead of applying whichever
        // delivery happens to arrive last. It runs outside the transaction —
        // holding the state-object lock across peer traffic would block every
        // node of this site, including this add's own retry bookkeeping.
        let local_deployment_id = current_deployment_id();
        let finalize_edit_path = peer_edit_path_with_fence(local_deployment_id.as_deref(), edit_generation);
        for target in state.peers.values() {
            if target.deployment_id == local_peer.deployment_id || same_identity_endpoint(&target.endpoint, &local_peer.endpoint)
            {
                continue;
            }
            let transport = match PeerTransport::for_runtime_peer(target).await {
                Ok(transport) => transport,
                Err(err) => {
                    initial_sync_errors.push(format!("{}: finalize peer sync state failed: {err}", target.endpoint));
                    continue;
                }
            };
            for peer in state.peers.values() {
                if let Err(err) = send_peer_admin_request_with_client(
                    &transport.client,
                    &transport.connection,
                    &finalize_edit_path,
                    &state.service_account_access_key,
                    &service_account_secret_key,
                    peer,
                )
                .await
                {
                    initial_sync_errors
                        .push(format!("{}: finalize sync state for {} failed: {err}", target.endpoint, peer.endpoint));
                }
            }
        }

        initial_sync_errors.extend(bootstrap_existing_metadata_after_add(&state, &local_peer, &service_account_secret_key).await);

        // Fix 1: back-fill pre-existing buckets so objects created before `replicate add`
        // are not silently left out of replication. Per-bucket failures are surfaced in the add
        // response below (BUG2) rather than swallowed; they do not abort the overall add.
        initial_sync_errors.extend(backfill_existing_buckets_after_add(&state, &local_peer, None).await);

        json_response(&ReplicateAddStatus {
            success: true,
            status: SITE_REPL_ADD_SUCCESS.to_string(),
            initial_sync_error_message: initial_sync_errors.render(),
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
        let _lifecycle_guard = SiteReplicationLifecycleGuard::acquire().await;
        // The request body is read before the bucket-op guard and the state
        // transaction: a client that stalls mid-body must hold neither the
        // state-object lock nor the write half of the bucket-op RwLock (which
        // would starve every bucket-operation hook in the meantime).
        let local_endpoint = site_replication_local_endpoint(&req.uri, &req.headers);
        let remove_req: SRRemoveReq = read_site_replication_json(req, "", false).await?;
        let (pending_remove, local_peer) = {
            let _bucket_op_guard = SITE_REPLICATION_BUCKET_OP_LOCK.write().await;
            update_site_replication_state_when_changed(move |state| {
                if pending_endpoint_refresh(state).is_some() {
                    return Err(s3_error!(InvalidRequest, "endpoint target refresh is pending"));
                }
                if state.pending_rotation.is_some() {
                    return Err(s3_error!(InvalidRequest, "service account rotation is pending"));
                }
                let local_peer = local_peer_at_endpoint(local_endpoint, state);

                // Resuming: the peers were already told about this pending
                // removal, so re-persisting the same record buys nothing.
                if let Some(pending) = state.pending_remove.clone() {
                    return Ok(StateCommit::Unchanged((pending, local_peer)));
                }

                validate_remove_sites_req(state, &remove_req)?;
                let service_account_access_key = state.service_account_access_key.clone();
                let secret_candidates = legacy_site_replicator_state_secret(state).into_iter().collect();
                let original_peers = state.peers.clone();
                let mut peer_remove_req = remove_req.clone();
                peer_remove_req.requesting_dep_id = local_peer.deployment_id.clone();
                *state = remove_sites(std::mem::take(state), remove_req);
                let pending = PendingRemove {
                    id: Uuid::new_v4().to_string(),
                    req: peer_remove_req,
                    service_account_access_key,
                    secret_candidates,
                    original_peers,
                    acked_deployment_ids: BTreeSet::new(),
                    updated_at: state.updated_at,
                };
                state.pending_remove = Some(pending.clone());
                Ok(StateCommit::Changed((pending, local_peer)))
            })
            .await?
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
                    &runtime_peer_connection(peer)?,
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
            let _bucket_op_guard = SITE_REPLICATION_BUCKET_OP_LOCK.write().await;
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

fn unsupported_site_netperf_result(endpoint: String) -> SiteNetPerfNodeResult {
    SiteNetPerfNodeResult {
        endpoint,
        tx: 0,
        tx_total_duration_ns: 0,
        rx: 0,
        rx_total_duration_ns: 0,
        total_conn: 0,
        error: "site-replication netperf is unsupported because RustFS does not perform peer traffic".to_string(),
    }
}

#[async_trait::async_trait]
impl Operation for SiteReplicationNetPerfHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        let endpoint = request_endpoint(&req.uri, &req.headers);
        Ok(go_gob_site_netperf_response(&unsupported_site_netperf_result(endpoint)))
    }
}

pub struct SRPeerJoinHandler {}

/// What the join admission decided about an incoming peer join. The verdict —
/// and the committed state the back-fill afterwards needs — travel out of
/// [`admit_peer_join`] instead of being answered where they are decided.
enum PeerJoinOutcome {
    Applied(Box<SiteReplicationState>, PeerInfo),
    /// A newer join already landed here; the sender is answered with the local
    /// peer record and nothing is written.
    Superseded(PeerInfo),
}

/// The serialized half of an accepted peer join: staleness check, IAM apply,
/// state commit.
///
/// The whole sequence runs under the lifecycle guard, and the staleness check
/// runs BEFORE `apply_iam`, against a load taken under that guard. Both
/// halves matter: the IAM write and the state commit cannot share a
/// transaction, so without the guard two joins accepted by this node can
/// interleave as "A checks a stale snapshot, B applies secret B and commits,
/// A overwrites IAM with secret A, A's commit is refused as superseded" —
/// leaving the persisted state advertising B's contract while IAM only
/// accepts A's secret, and every peer control-plane call failing auth. The
/// closing transaction still re-checks staleness: another NODE of this site
/// can accept a join concurrently (the guard is process-local, exactly as
/// far as the pre-P1-15 process mutex reached), and the state-object lock is
/// what arbitrates that.
///
/// `apply_iam` is injected so the interleaving regression test can gate it
/// mid-flight; production passes the real service-account upsert.
async fn admit_peer_join<F, Fut>(
    local_endpoint: String,
    join_req: SRPeerJoinReq,
    defer_sync_state_enable: bool,
    apply_iam: F,
) -> S3Result<PeerJoinOutcome>
where
    F: FnOnce(SRPeerJoinReq) -> Fut,
    Fut: std::future::Future<Output = S3Result<()>>,
{
    let _lifecycle_guard = SiteReplicationLifecycleGuard::acquire().await;

    let fresh = load_site_replication_state().await?;
    let fresh_local_peer = local_peer_at_endpoint(local_endpoint.clone(), &fresh);
    if join_request_is_superseded(&fresh, join_req.updated_at) {
        let peer = fresh
            .peers
            .get(&fresh_local_peer.deployment_id)
            .cloned()
            .unwrap_or(fresh_local_peer);
        return Ok(PeerJoinOutcome::Superseded(peer));
    }

    apply_iam(join_req.clone()).await?;

    let incoming_updated_at = join_req.updated_at;
    update_site_replication_state_when_changed(move |state| {
        let local_peer = local_peer_at_endpoint(local_endpoint, state);
        if join_request_is_superseded(state, incoming_updated_at) {
            let peer = state.peers.get(&local_peer.deployment_id).cloned().unwrap_or(local_peer);
            return Ok(StateCommit::Unchanged(PeerJoinOutcome::Superseded(peer)));
        }
        apply_peer_join(state, &local_peer, join_req, defer_sync_state_enable);
        Ok(StateCommit::Changed(PeerJoinOutcome::Applied(Box::new(state.clone()), local_peer)))
    })
    .await
}

/// Upsert the replication service account a peer join carries. No-op when the
/// join brings no credentials.
async fn apply_peer_join_service_account(join_req: SRPeerJoinReq) -> S3Result<()> {
    if join_req.svc_acct_access_key.is_empty() || join_req.svc_acct_secret_key.is_empty() {
        return Ok(());
    }
    let Some(iam_sys) = current_iam_handle() else {
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
                    parent_user: None,
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
    Ok(())
}

#[async_trait::async_trait]
impl Operation for SRPeerJoinHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationAddAction).await?;
        let bootstrap_token = site_replication_bootstrap_token(&req.uri);
        let local_endpoint = site_replication_local_endpoint(&req.uri, &req.headers);
        // The body is fully read before the admission takes the lifecycle
        // guard: a sender that stalls mid-body must not block this node's
        // add/remove/rotate/reconciler.
        let join_envelope: SRPeerJoinEnvelope = read_site_replication_json(req, &cred.secret_key, true).await?;
        let defer_sync_state_enable = join_envelope.defer_sync_state_enable;
        let join_req = join_envelope.request;
        validate_join_peer_snapshot(&join_req.peers)?;

        let committed =
            admit_peer_join(local_endpoint, join_req, defer_sync_state_enable, apply_peer_join_service_account).await?;
        // Committed; the reverse-reachability probe and the bucket back-fill
        // run outside the transaction — their transport helpers' retry-event
        // bookkeeping re-enters it (P1-15).
        let (state, local_peer) = match committed {
            PeerJoinOutcome::Applied(state, local_peer) => (*state, local_peer),
            PeerJoinOutcome::Superseded(peer) => {
                return json_response(&SRPeerJoinResponse {
                    peer,
                    ..Default::default()
                });
            }
        };
        // Fix 1 (receiving side): ensure the joining peer also sets up replication for any
        // buckets it already owns so the reverse direction works from the start. Per-bucket
        // failures are logged (BUG2) so a reverse-direction back-fill gap is observable.
        let mut backfill_errors = probe_reverse_peer_reachability(&state, &local_peer).await;
        backfill_errors.extend(backfill_existing_buckets_after_add(&state, &local_peer, bootstrap_token.as_deref()).await);
        if !backfill_errors.is_empty() {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                result = "join_backfill_incomplete",
                error_count = backfill_errors.total,
                reported_error_count = backfill_errors.reported(),
                "admin site replication state"
            );
        }
        json_response(&SRPeerJoinResponse {
            peer: state.peers.get(&local_peer.deployment_id).cloned().unwrap_or(local_peer),
            initial_sync_error_message: backfill_errors.render(),
        })
    }
}

pub struct SRPeerBucketOpsHandler {}

#[async_trait::async_trait]
impl Operation for SRPeerBucketOpsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        let _bucket_op_guard = SITE_REPLICATION_BUCKET_OP_LOCK.read().await;
        let state = load_site_replication_state().await?;
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
        if state.pending_remove.is_some()
            || (!state.enabled()
                && !bootstrap_peer_bucket_operation_allowed(
                    &bucket,
                    &operation,
                    queries.get("bootstrapToken").map(String::as_str),
                ))
        {
            return Err(s3_error!(InvalidRequest, "site replication is not enabled"));
        }

        let Some(store) = object_store_from_req(&req) else {
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
                let expected_incarnation_id = metadata_sys::capture_bucket_metadata_incarnation(&bucket)
                    .await
                    .map_err(ApiError::from)?;
                metadata_sys::update_if_incarnation(
                    &bucket,
                    BUCKET_VERSIONING_CONFIG,
                    bucket_versioning_xml()?,
                    expected_incarnation_id,
                )
                .await
                .map_err(ApiError::from)?;
            }
            "configure-replication" => {
                store
                    .get_bucket_info(&bucket, &BucketOptions::default())
                    .await
                    .map_err(ApiError::from)?;
                ensure_site_replication_bucket_setup(&bucket).await?;
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
        let body = read_site_replication_body(req, &cred.secret_key, true).await?;
        let (mut incoming, tls_presence) = parse_public_peer_edit(&body)?;
        // Planning snapshot: every commit below re-loads the state inside its
        // transaction and re-checks the `updated_at` / pending-refresh
        // precondition there, because the peer probes and fan-outs in between
        // must not run under the state-object lock.
        let current_state = load_site_replication_state().await?;
        apply_public_peer_edit_tls_presence(&current_state, &mut incoming, tls_presence);
        if !incoming.deployment_id.is_empty() || !incoming.endpoint.is_empty() || !incoming.name.is_empty() {
            validate_proposed_peer(&incoming)?;
        }
        if current_state.pending_rotation.is_some() || current_state.pending_remove.is_some() {
            return Err(s3_error!(InvalidRequest, "another site replication operation is pending"));
        }
        let persisted_pending = pending_endpoint_refresh(&current_state);
        let endpoint_refresh_requested = peer_endpoint_refresh_requested(&current_state, &incoming);
        if persisted_pending.is_some() && !endpoint_refresh_requested {
            return Err(s3_error!(InvalidRequest, "an endpoint target refresh is already pending"));
        }
        let pending = endpoint_refresh_requested.then(|| {
            persisted_pending.clone().unwrap_or_else(|| PendingEndpointRefresh {
                id: Uuid::new_v4().to_string(),
                peer: normalize_peer_info(incoming.clone()),
                remote_peers: current_state.peers.clone(),
                acked_deployment_ids: BTreeSet::new(),
            })
        });
        // The precondition every commit below re-checks: the topology this
        // edit was planned against, and the endpoint refresh it either
        // continues or requires the absence of.
        let expected_updated_at = current_state.updated_at;
        let expected_pending_id = persisted_pending.as_ref().map(|pending| pending.id.clone());
        let local_peer = current_local_runtime_peer(&current_state);
        let existing_peer = existing_peer_for_edit(&current_state, &incoming);
        let tls_capability_required = edit_peer_tls_capability_required(existing_peer, &incoming);
        let tls_transport_probe_required = peer_tls_settings_changed(existing_peer, &incoming);
        let mut service_account_secret_key = None;
        if tls_capability_required || tls_transport_probe_required {
            if current_state.service_account_access_key.is_empty() {
                return Err(s3_error!(InvalidRequest, "site replication service account is not configured"));
            }
            let secret = site_replicator_service_account_secret(&current_state.service_account_access_key).await?;
            if tls_capability_required {
                require_edit_peer_tls_capability(
                    &current_state,
                    &incoming,
                    &local_peer,
                    &current_state.service_account_access_key,
                    &secret,
                )
                .await?;
            }
            if tls_transport_probe_required {
                probe_proposed_peer_tls_transport(&incoming, &current_state.service_account_access_key, &secret).await?;
            }
            // Early exit on a state that moved under the probe. Advisory only:
            // the binding check is the CAS inside whichever commit follows.
            let latest_state = load_site_replication_state().await?;
            ensure_edit_precondition(&latest_state, expected_updated_at, expected_pending_id.as_ref(), "capability probe")?;
            service_account_secret_key = Some(secret);
        }
        if endpoint_refresh_requested && current_state.service_account_access_key.is_empty() {
            return Err(s3_error!(InvalidRequest, "site replication service account is not configured"));
        }
        if current_state.service_account_access_key.is_empty() {
            // No peers to notify: the edit is the whole operation, so it is
            // computed and committed in one transaction.
            let incoming = incoming.clone();
            update_site_replication_state(move |state| {
                ensure_edit_precondition(state, expected_updated_at, expected_pending_id.as_ref(), "the edit")?;
                *state = edit_state(std::mem::take(state), incoming, ilm_expiry_override);
                Ok(())
            })
            .await?;
        } else {
            let service_account_secret_key = match service_account_secret_key {
                Some(secret) => secret,
                None => site_replicator_service_account_secret(&current_state.service_account_access_key).await?,
            };
            let routing_peers = pending
                .as_ref()
                .map(|pending| &pending.remote_peers)
                .unwrap_or(&current_state.peers);
            let local_deployment_id = current_deployment_id();
            let remote_targets = endpoint_refresh_remote_targets(routing_peers, pending.as_ref(), local_deployment_id.as_deref());

            if endpoint_refresh_requested {
                let pending = pending.clone().ok_or_else(|| {
                    S3Error::with_message(S3ErrorCode::InternalError, "endpoint refresh state is missing".to_string())
                })?;
                let probes = futures::future::join_all(remote_targets.iter().map(|target| {
                    send_endpoint_refresh_admin_request_raw(
                        target,
                        &pending,
                        SITE_REPLICATION_PEER_EDIT_CAPABILITY_PATH,
                        &current_state.service_account_access_key,
                        &service_account_secret_key,
                        &(),
                    )
                }))
                .await;
                let mut legacy_deployment_ids = BTreeSet::new();
                for (target, probe) in remote_targets.iter().zip(probes) {
                    let (status, body) = probe.map_err(|err| {
                        S3Error::with_message(
                            S3ErrorCode::InternalError,
                            format!("probe endpoint target refresh on peer {} failed: {err}", target.endpoint),
                        )
                    })?;
                    if endpoint_refresh_capability_supported(target, status, &body)? {
                        continue;
                    } else {
                        legacy_deployment_ids.insert(target.deployment_id.clone());
                    }
                }

                let pending_id = pending.id.clone();
                let refresh_request = EndpointRefreshRequest {
                    id: pending.id.clone(),
                    peer: pending.peer.clone(),
                };
                // Announce the pending refresh. The CAS sits in the same
                // transaction as the write it guards, so a topology change
                // that landed during the capability probes above cannot be
                // overwritten by this snapshot.
                let expected_pending_id = expected_pending_id.clone();
                let pending = update_site_replication_state(move |state| {
                    ensure_edit_precondition(state, expected_updated_at, expected_pending_id.as_ref(), "capability probe")?;
                    let pending = merge_pending_endpoint_refresh(state, &pending, std::iter::empty::<String>())?;
                    set_pending_endpoint_refresh(state, pending.clone())?;
                    Ok(pending)
                })
                .await?;
                let responses = futures::future::join_all(remote_targets.iter().map(|target| async {
                    if legacy_deployment_ids.contains(&target.deployment_id) {
                        refresh_legacy_peer_bucket_targets(
                            target,
                            &pending,
                            &current_state.service_account_access_key,
                            &service_account_secret_key,
                        )
                        .await
                    } else {
                        let body = send_endpoint_refresh_admin_request(
                            target,
                            &pending,
                            SITE_REPLICATION_PEER_EDIT_REFRESH_PATH,
                            &current_state.service_account_access_key,
                            &service_account_secret_key,
                            &refresh_request,
                        )
                        .await?;
                        parse_endpoint_refresh_status(target, &body)
                    }
                }))
                .await;
                let mut acked_deployment_ids = BTreeSet::new();
                let mut refresh_error = None;
                for (target, response) in remote_targets.iter().zip(responses) {
                    match response {
                        Ok(()) => {
                            acked_deployment_ids.insert(target.deployment_id.clone());
                        }
                        Err(err) if refresh_error.is_none() => refresh_error = Some(err),
                        Err(_) => {}
                    }
                }

                let acked_pending_id = pending_id.clone();
                let service_account_access_key = update_site_replication_state(move |state| {
                    let Some(pending) = pending_endpoint_refresh(state).filter(|pending| pending.id == acked_pending_id) else {
                        return Err(s3_error!(InvalidRequest, "endpoint target refresh state changed during update"));
                    };
                    let pending = merge_pending_endpoint_refresh(state, &pending, acked_deployment_ids)?;
                    set_pending_endpoint_refresh(state, pending)?;
                    Ok(state.service_account_access_key.clone())
                })
                .await?;
                if let Some(err) = refresh_error {
                    return Err(err);
                }
                let service_account_secret_key = site_replicator_service_account_secret(&service_account_access_key).await?;
                refresh_bucket_targets_after_endpoint_edit(&pending_id, &service_account_secret_key).await?;
                update_site_replication_state(move |state| {
                    let Some(pending) = pending_endpoint_refresh(state).filter(|pending| pending.id == pending_id) else {
                        return Err(s3_error!(InvalidRequest, "endpoint target refresh state changed during update"));
                    };
                    *state = edit_state(std::mem::take(state), pending.peer, ilm_expiry_override);
                    clear_pending_endpoint_refresh(state);
                    Ok(())
                })
                .await?;
            } else {
                // Commit before the peer fan-out (mirrors the add/join
                // handlers): a failed notification is recorded as a retry
                // event and converges from the committed local state —
                // fanning out first meant the retry event pointed at a state
                // the local site had not saved. The edit itself is applied to
                // the state the transaction loads, under the CAS, so a
                // topology change that slipped past the planning snapshot
                // fails the edit instead of being overwritten by it. The
                // generation is allocated in that same commit, i.e. under the
                // state-object lock, so it orders this edit against one
                // another node of this site accepts concurrently.
                let incoming = incoming.clone();
                let (edit_generation, peers_to_send) = update_site_replication_state(move |state| {
                    ensure_edit_precondition(state, expected_updated_at, expected_pending_id.as_ref(), "the edit")?;
                    *state = edit_state(std::mem::take(state), incoming.clone(), ilm_expiry_override);
                    let peers_to_send: Vec<PeerInfo> = if ilm_expiry_override.is_some() {
                        state.peers.values().cloned().collect()
                    } else {
                        vec![normalize_peer_info(incoming)]
                    };
                    Ok((next_peer_edit_generation(state), peers_to_send))
                })
                .await?;
                let edit_path = peer_edit_path_with_fence(local_deployment_id.as_deref(), edit_generation);
                let delivery_fence = local_deployment_id.is_some().then_some(edit_generation);

                // The fan-out runs outside the transaction — peer traffic
                // under the state-object lock would stall every writer of this
                // site, and the retry bookkeeping below re-enters it (P1-15).
                // Ordering is the generation fence's job: a delivery this
                // fan-out is still retrying is rejected by the receiver once a
                // newer generation from this site has landed there.
                let mut delivered: Vec<PeerInfo> = Vec::new();
                let mut failure: Option<(PeerInfo, S3Error)> = None;
                'fanout: for target in remote_targets {
                    let transport = PeerTransport::for_runtime_peer(target).await?;
                    for peer in &peers_to_send {
                        if let Err(err) = send_peer_admin_request_with_client(
                            &transport.client,
                            &transport.connection,
                            &edit_path,
                            &current_state.service_account_access_key,
                            &service_account_secret_key,
                            peer,
                        )
                        .await
                        {
                            failure = Some((target.clone(), err));
                            break 'fanout;
                        }
                    }
                    delivered.push(target.clone());
                }

                // Settle only what this generation is entitled to: a newer
                // edit that committed and failed its own delivery while this
                // fan-out was in flight left a retry event that must survive.
                for target in &delivered {
                    dequeue_site_replication_retry_event_for_generation(target, SITE_REPLICATION_PEER_EDIT_PATH, delivery_fence)
                        .await;
                }
                if let Some((target, err)) = failure {
                    enqueue_site_replication_retry_event_for_generation(
                        &target,
                        SITE_REPLICATION_PEER_EDIT_PATH,
                        &err,
                        delivery_fence,
                    )
                    .await;
                    return Err(err);
                }
            }
        }

        json_response(&ReplicateEditStatus {
            success: true,
            status: SITE_REPL_EDIT_SUCCESS.to_string(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
    }
}

pub struct SRPeerEditCapabilitiesHandler {}

#[async_trait::async_trait]
impl Operation for SRPeerEditCapabilitiesHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        json_response(&ReplicateEditStatus {
            success: query_pairs(&req.uri)
                .get("capability")
                .is_some_and(|value| peer_edit_capability_supported(value)),
            status: SITE_REPL_EDIT_SUCCESS.to_string(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        })
    }
}

pub struct SRPeerEditHandler {}

/// What the peer-edit transaction decided about an incoming delivery. The
/// checks and the write share one transaction, so the verdict has to travel
/// out of the closure instead of being answered where it is taken.
enum PeerEditOutcome {
    /// Applied; carries the service account access key the follow-up
    /// endpoint-refresh work needs from the committed state.
    Applied(String),
    /// Nothing to do — a superseded delivery or one this site already
    /// committed. Answered as success so the sender stops retrying.
    Acked,
    /// Refused, with the detail the sender is told.
    Rejected(&'static str),
}

#[async_trait::async_trait]
impl Operation for SRPeerEditHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        let queries = query_pairs(&req.uri);
        let ilm_expiry_override = sr_edit_ilm_expiry_override(&req.uri);
        let endpoint_refresh_requested = queries.get("refresh-targets").is_some_and(|value| value == "true");
        let commit_fence = peer_edit_fence(&queries);
        let local_endpoint = site_replication_local_endpoint(&req.uri, &req.headers);
        let (refresh_id, incoming) = if endpoint_refresh_requested {
            let refresh: EndpointRefreshRequest = read_site_replication_json(req, "", false).await?;
            (Some(refresh.id), refresh.peer)
        } else {
            (None, read_site_replication_json(req, "", false).await?)
        };

        // Everything the delivery is checked against — the fence, the pending
        // operations, the peer it names — is read inside the transaction that
        // applies it. Checking against a state loaded before the lock would
        // let the check pass on one snapshot and the write land on another.
        let commit_endpoint = local_endpoint.clone();
        let commit_refresh_id = refresh_id.clone();
        let outcome = update_site_replication_state_when_changed(move |state| {
            let mut incoming = incoming;
            let local_peer = local_peer_at_endpoint(commit_endpoint, state);
            // Ordering fence: the sending site allocates the generation under
            // its state-object lock, so a delivery that lost the race carries
            // a generation this site has already passed. Applying it would
            // roll the peer back to the older edit. Ack it — the newer edit
            // already landed, so the sender has nothing to retry.
            if let Some((origin, generation)) = commit_fence.as_ref()
                && peer_edit_delivery_is_stale(state, origin, *generation)
            {
                return Ok(StateCommit::Unchanged(PeerEditOutcome::Acked));
            }
            if endpoint_refresh_requested && (state.pending_rotation.is_some() || state.pending_remove.is_some()) {
                return Ok(StateCommit::Unchanged(PeerEditOutcome::Rejected(
                    "another site replication operation is pending",
                )));
            }
            if same_identity_endpoint(&incoming.endpoint, &local_peer.endpoint) {
                incoming.deployment_id = local_peer.deployment_id.clone();
                if incoming.name.is_empty() {
                    incoming.name = local_peer.name.clone();
                }
            }
            align_peer_edit_deployment_id(state, &mut incoming);
            if endpoint_refresh_requested
                && pending_endpoint_refresh(state).is_some_and(|pending| commit_refresh_id.as_deref() != Some(&pending.id))
            {
                return Ok(StateCommit::Unchanged(PeerEditOutcome::Rejected(
                    "another endpoint target refresh is pending",
                )));
            }
            if endpoint_refresh_requested
                && (commit_refresh_id.as_ref().is_none_or(String::is_empty) || !peer_endpoint_edit_requested(state, &incoming))
            {
                return Ok(StateCommit::Unchanged(PeerEditOutcome::Rejected("peer endpoint was not found")));
            }
            if endpoint_refresh_requested && internal_endpoint_refresh_already_committed(state, &incoming) {
                return Ok(StateCommit::Unchanged(PeerEditOutcome::Acked));
            }

            if endpoint_refresh_requested {
                validate_proposed_peer(&incoming)?;
                set_pending_endpoint_refresh(
                    state,
                    PendingEndpointRefresh {
                        id: commit_refresh_id.unwrap_or_default(),
                        peer: incoming,
                        remote_peers: BTreeMap::new(),
                        acked_deployment_ids: BTreeSet::new(),
                    },
                )?;
            } else {
                *state = apply_internal_peer_edit(std::mem::take(state), &local_peer, incoming, ilm_expiry_override)?;
            }
            // Raise the origin's high-water mark in the same commit as the
            // edit it fences: a crash between the two would let the superseded
            // delivery apply on the next attempt.
            if let Some((origin, generation)) = commit_fence.as_ref() {
                record_applied_peer_edit_generation(state, origin, *generation);
            }
            Ok(StateCommit::Changed(PeerEditOutcome::Applied(state.service_account_access_key.clone())))
        })
        .await?;

        let service_account_access_key = match outcome {
            PeerEditOutcome::Applied(service_account_access_key) => service_account_access_key,
            PeerEditOutcome::Acked => {
                return json_response(&ReplicateEditStatus {
                    success: true,
                    status: SITE_REPL_EDIT_SUCCESS.to_string(),
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                    ..Default::default()
                });
            }
            PeerEditOutcome::Rejected(err_detail) => {
                return json_response(&ReplicateEditStatus {
                    success: false,
                    status: SITE_REPL_EDIT_SUCCESS.to_string(),
                    err_detail: err_detail.to_string(),
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                });
            }
        };
        if endpoint_refresh_requested {
            if service_account_access_key.is_empty() {
                return json_response(&ReplicateEditStatus {
                    success: false,
                    status: SITE_REPL_EDIT_SUCCESS.to_string(),
                    err_detail: "site replicator service account is not configured".to_string(),
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                });
            }
            let service_account_secret_key = site_replicator_service_account_secret(&service_account_access_key).await?;
            let pending_id = refresh_id.unwrap_or_default();
            // The bucket-target rewrite talks to the store for every bucket;
            // it runs between the two transactions, never inside one.
            refresh_bucket_targets_after_endpoint_edit(&pending_id, &service_account_secret_key).await?;
            let committed = update_site_replication_state_when_changed(move |state| {
                let local_peer = local_peer_at_endpoint(local_endpoint, state);
                let Some(pending) = pending_endpoint_refresh(state).filter(|pending| pending.id == pending_id) else {
                    return Ok(StateCommit::Unchanged(false));
                };
                *state = apply_internal_peer_edit(std::mem::take(state), &local_peer, pending.peer, ilm_expiry_override)?;
                clear_pending_endpoint_refresh(state);
                Ok(StateCommit::Changed(true))
            })
            .await?;
            if !committed {
                return json_response(&ReplicateEditStatus {
                    success: false,
                    status: SITE_REPL_EDIT_SUCCESS.to_string(),
                    err_detail: "endpoint target refresh state changed during update".to_string(),
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                });
            }
            return json_response(&ReplicateEditStatus {
                success: true,
                status: SITE_REPL_EDIT_SUCCESS.to_string(),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
                ..Default::default()
            });
        }
        Ok(empty_response(StatusCode::OK))
    }
}

pub struct SRPeerRemoveHandler {}

#[async_trait::async_trait]
impl Operation for SRPeerRemoveHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationRemoveAction).await?;
        let remove_req: SRRemoveReq = read_site_replication_json(req, "", false).await?;
        let _lifecycle_guard = SiteReplicationLifecycleGuard::acquire().await;
        let _bucket_op_guard = SITE_REPLICATION_BUCKET_OP_LOCK.write().await;
        let removed_deployment_ids = update_site_replication_state(move |state| {
            if pending_endpoint_refresh(state).is_some() {
                return Err(s3_error!(InvalidRequest, "endpoint target refresh is pending"));
            }
            if state.pending_rotation.is_some() {
                return Err(s3_error!(InvalidRequest, "service account rotation is pending"));
            }

            let removed_deployment_ids = removed_deployment_ids_for_remove_req(state, &remove_req);
            *state = remove_sites(std::mem::take(state), remove_req);
            Ok(removed_deployment_ids)
        })
        .await?;

        // Clean up bucket targets and replication rules that referenced removed peers.
        if !removed_deployment_ids.is_empty()
            && let Err(err) = cleanup_removed_site_replication_buckets(&removed_deployment_ids).await
        {
            warn!(
                event = EVENT_ADMIN_SITE_REPLICATION_STATE,
                component = LOG_COMPONENT_ADMIN,
                subsystem = LOG_SUBSYSTEM_SITE_REPLICATION,
                result = "peer_remove_bucket_cleanup_failed",
                error = ?err,
                "admin site replication state"
            );
        }

        Ok(empty_response(StatusCode::OK))
    }
}

pub struct SiteReplicationResyncOpHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationResyncOpHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        validate_site_replication_admin_request(&req, AdminAction::SiteReplicationResyncAction).await?;
        let query = query_pairs(&req.uri);
        let operation = query.get("operation").cloned().unwrap_or_default();
        let resolved_store = object_store_from_req(&req);
        let requested_peer: PeerInfo = read_site_replication_json(req, "", false).await?;
        let _lifecycle_guard = SiteReplicationLifecycleGuard::acquire().await;
        let (peer, existing_status) = {
            let state = load_site_replication_state().await?;
            let local_peer = current_local_runtime_peer(&state);
            let requested_peer = normalize_peer_info(requested_peer);
            if requested_peer.deployment_id == local_peer.deployment_id {
                return Err(s3_error!(InvalidRequest, "invalid peer specified - cannot resync to self"));
            }
            let peer = state
                .peers
                .get(&requested_peer.deployment_id)
                .cloned()
                .ok_or_else(|| s3_error!(InvalidRequest, "site replication peer not found"))?;
            (peer, state.resync_status.get(&requested_peer.deployment_id).cloned())
        };

        let mut status = match operation.as_str() {
            SITE_REPL_RESYNC_START => {
                if let Some(existing) = existing_status.as_ref() {
                    let existing = refresh_site_resync_status(existing.clone(), &peer).await;
                    persist_site_resync_status(&peer.deployment_id, &existing).await?;
                    if site_resync_is_active(&existing) {
                        return Err(s3_error!(InvalidRequest, "site replication resync is already active"));
                    }
                }
                let Some(store) = resolved_store else {
                    return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
                };
                let mut bucket_names: Vec<String> = store
                    .list_bucket(&BucketOptions::default())
                    .await
                    .map_err(ApiError::from)?
                    .into_iter()
                    .map(|bucket| bucket.name)
                    .collect();
                bucket_names.sort();
                let now = OffsetDateTime::now_utc();
                let mut bucket_statuses = Vec::with_capacity(bucket_names.len());
                for bucket in bucket_names {
                    bucket_statuses.push(site_bucket_resync_manifest_entry(&bucket, &peer, now).await);
                }
                let mut status = SRResyncOpStatus {
                    op_type: SITE_REPL_RESYNC_START.to_string(),
                    resync_id: Uuid::new_v4().to_string(),
                    status: "success".to_string(),
                    state: "pending".to_string(),
                    buckets: bucket_statuses,
                    created_at: Some(now),
                    started_at: Some(now),
                    updated_at: Some(now),
                    generation: existing_status
                        .as_ref()
                        .map_or(1, |existing| existing.generation.saturating_add(1).max(1)),
                    ..Default::default()
                };
                summarize_site_resync_status(&mut status, now);
                persist_new_site_resync_status(&peer.deployment_id, &status).await?;
                for index in 0..status.buckets.len() {
                    if status.buckets[index].target_arn.is_empty() || status.buckets[index].status == "failed" {
                        continue;
                    }
                    let previous = status.buckets[index].clone();
                    let mut result = start_site_bucket_resync(&previous.bucket, &previous.target_arn, &status.resync_id).await;
                    result.created_at = previous.created_at;
                    result.started_at = Some(OffsetDateTime::now_utc());
                    result.updated_at = result.started_at;
                    result.generation = status.generation;
                    result.err_detail = summarize_peer_error_detail(&result.err_detail);
                    status.buckets[index] = result;
                    summarize_site_resync_status(&mut status, OffsetDateTime::now_utc());
                    persist_site_resync_status(&peer.deployment_id, &status).await?;
                }
                status = refresh_site_resync_status(status, &peer).await;
                persist_site_resync_status(&peer.deployment_id, &status).await?;
                status
            }
            SITE_REPL_RESYNC_CANCEL => {
                let Some(existing_status) = existing_status else {
                    return Err(s3_error!(InvalidRequest, "no resync in progress"));
                };
                if existing_status.resync_id.is_empty() {
                    return Err(s3_error!(InvalidRequest, "no resync in progress"));
                }
                let mut status = refresh_site_resync_status(existing_status, &peer).await;
                if status.buckets.iter().any(|bucket| bucket.status == "conflict") {
                    return Err(s3_error!(
                        InvalidRequest,
                        "site replication resync target belongs to a different active operation"
                    ));
                }
                if site_resync_cancel_is_idempotent(&status) {
                    status.op_type = SITE_REPL_RESYNC_CANCEL.to_string();
                    for bucket in &status.buckets {
                        if !bucket.target_arn.is_empty() {
                            let _ = cancel_site_bucket_resync(&bucket.bucket, &bucket.target_arn, &status.resync_id).await;
                        }
                    }
                    status
                } else {
                    if !site_resync_is_active(&status) {
                        return Err(s3_error!(InvalidRequest, "no active resync to cancel"));
                    }
                    status.op_type = SITE_REPL_RESYNC_CANCEL.to_string();
                    status.state = "canceling".to_string();
                    status.updated_at = Some(OffsetDateTime::now_utc());
                    persist_site_resync_status(&peer.deployment_id, &status).await?;
                    for index in 0..status.buckets.len() {
                        if status.buckets[index].target_arn.is_empty()
                            || matches!(status.buckets[index].status.as_str(), "failed" | "canceled")
                        {
                            continue;
                        }
                        let previous = status.buckets[index].clone();
                        let mut result =
                            cancel_site_bucket_resync(&previous.bucket, &previous.target_arn, &status.resync_id).await;
                        result.created_at = previous.created_at;
                        result.started_at = previous.started_at;
                        result.updated_at = Some(OffsetDateTime::now_utc());
                        result.completed_at = result.updated_at;
                        result.generation = status.generation;
                        result.err_detail = summarize_peer_error_detail(&result.err_detail);
                        status.buckets[index] = result;
                        summarize_site_resync_status(&mut status, OffsetDateTime::now_utc());
                        persist_site_resync_status(&peer.deployment_id, &status).await?;
                    }
                    status = refresh_site_resync_status(status, &peer).await;
                    persist_site_resync_status(&peer.deployment_id, &status).await?;
                    status
                }
            }
            SITE_REPL_RESYNC_STATUS => {
                let status = existing_status.unwrap_or_else(|| SRResyncOpStatus {
                    op_type: SITE_REPL_RESYNC_STATUS.to_string(),
                    status: "not-found".to_string(),
                    ..Default::default()
                });
                if status.resync_id.is_empty() {
                    status
                } else {
                    let status = refresh_site_resync_status(status, &peer).await;
                    persist_site_resync_status(&peer.deployment_id, &status).await?;
                    status
                }
            }
            _ => return Err(s3_error!(InvalidRequest, "unsupported resync operation")),
        };
        status
            .buckets
            .sort_by(|left, right| left.bucket.cmp(&right.bucket).then(left.target_arn.cmp(&right.target_arn)));
        let (limit, offset) = parse_site_resync_page(&query, &status)?;
        json_response(&site_resync_page(&status, limit, offset)?)
    }
}

pub struct SRStateEditHandler {}

#[async_trait::async_trait]
impl Operation for SRStateEditHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        reject_site_replicator_on_public_admin(&cred)?;
        let body: SRStateEditReq = read_site_replication_json(req, "", false).await?;
        update_site_replication_state(move |state| {
            *state = apply_state_edit_req(std::mem::take(state), body);
            Ok(())
        })
        .await?;
        Ok(empty_response(StatusCode::OK))
    }
}

pub struct SiteReplicationRepairHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationRepairHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        reject_site_replicator_on_public_admin(&cred)?;
        let state = load_site_replication_state().await?;
        if !state.enabled() || state.service_account_access_key.is_empty() {
            return Err(s3_error!(InvalidRequest, "site replication is not configured"));
        }
        let local_peer = current_local_peer(&req, &state);
        let body: SiteReplicationRepairRequest = read_site_replication_json(req, "", false).await?;
        let info = build_sr_info(&state, &local_peer).await?;
        let plan = site_replication_bootstrap_plan(&info)?;
        let signing_key = current_token_signing_key().ok_or_else(|| {
            S3Error::with_message(S3ErrorCode::InternalError, "token signing key is not initialized".to_string())
        })?;
        let preflight_token = site_replication_repair_preflight_token(&state, &plan, signing_key.as_bytes())?;
        let sites = site_replication_repair_sites(&state, &local_peer, &plan, signing_key.as_bytes())?;

        if body.mode == SiteReplicationRepairMode::DryRun {
            if body.preflight_token.is_some() || body.operation_id.is_some() {
                return Err(s3_error!(InvalidRequest, "dry-run does not accept preflightToken or operationId"));
            }
            return json_response(&SiteReplicationRepairPreflight {
                mode: "dry-run",
                status: "planned",
                preflight_token,
                retry_events: state
                    .retry_queue
                    .iter()
                    .filter(|event| retry_event_replayed_by_bootstrap(event))
                    .count(),
                sites,
            });
        }

        let supplied_token = body
            .preflight_token
            .as_deref()
            .filter(|token| {
                token.len() == 43
                    && token
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            })
            .ok_or_else(|| s3_error!(InvalidRequest, "execute requires a valid preflightToken"))?;
        let operation_id = match body.operation_id {
            Some(id) => Uuid::parse_str(&id)
                .map_err(|_| s3_error!(InvalidRequest, "operationId must be a UUID"))?
                .to_string(),
            None => Uuid::new_v4().to_string(),
        };
        execute_site_replication_repair(SiteReplicationRepairExecutionRequest {
            local_peer,
            preflight_token: supplied_token.to_string(),
            operation_id,
            signing_key,
        })
        .await
    }
}

pub struct SiteReplicationRepairStatusHandler {}

#[async_trait::async_trait]
impl Operation for SiteReplicationRepairStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let cred = validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;
        reject_site_replicator_on_public_admin(&cred)?;
        let operation_id = req
            .uri
            .query()
            .and_then(|query| {
                form_urlencoded::parse(query.as_bytes())
                    .find_map(|(key, value)| (key == "operation-id").then(|| value.into_owned()))
            })
            .ok_or_else(|| s3_error!(InvalidRequest, "operation-id is required"))?;
        let operation_id = Uuid::parse_str(&operation_id)
            .map_err(|_| s3_error!(InvalidRequest, "operation-id must be a UUID"))?
            .to_string();
        let operation = read_site_replication_repair_state()
            .await?
            .operations
            .get(&operation_id)
            .cloned()
            .ok_or_else(|| s3_error!(InvalidRequest, "repair operation was not found"))?;
        json_response(&site_replication_repair_operation_response(&operation))
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
        // The lifecycle guard is what keeps the rotation's IAM writes and the
        // background service-account reconciler apart: the reconciler runs
        // its whole repair under a lifecycle try-acquire, and its
        // pending-rotation precheck is only sound if a rotation cannot start
        // mid-repair and race its own IAM write against the reconciler's
        // stale one. (The removed process mutex used to provide this
        // exclusion as a side effect.)
        let _lifecycle_guard = SiteReplicationLifecycleGuard::acquire().await;
        let local_endpoint = site_replication_local_endpoint(&req.uri, &req.headers);
        let rotation_parent = cred.access_key.clone();
        let (pending_rotation, local_peer, previous_access_key) = update_site_replication_state_when_changed(move |state| {
            if !state.enabled() {
                return Err(s3_error!(InvalidRequest, "site replication is not configured"));
            }
            if pending_endpoint_refresh(state).is_some() {
                return Err(s3_error!(InvalidRequest, "endpoint target refresh is pending"));
            }
            if state.pending_remove.is_some() {
                return Err(s3_error!(InvalidRequest, "site replication remove is pending"));
            }
            let local_peer = local_peer_at_endpoint(local_endpoint, state);
            let previous_access_key = state.service_account_access_key.clone();

            // Resuming a rotation another attempt already recorded must
            // not rewrite the state: the pending record is the contract
            // the peers were told about.
            if let Some(pending) = state.pending_rotation.clone() {
                return Ok(StateCommit::Unchanged((pending, local_peer, previous_access_key)));
            }

            let new_secret_key = rustfs_credentials::gen_secret_key(40)
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("generate secret key failed: {e}")))?;
            state.service_account_access_key = SITE_REPLICATOR_SERVICE_ACCOUNT.to_string();
            state.service_account_parent = rotation_parent.clone();
            state.updated_at = Some(OffsetDateTime::now_utc());
            let pending = PendingRotation {
                id: Uuid::new_v4().to_string(),
                access_key: SITE_REPLICATOR_SERVICE_ACCOUNT.to_string(),
                parent: rotation_parent,
                new_secret_key,
                secret_candidates: legacy_site_replicator_state_secret(state).into_iter().collect(),
                peers: state.peers.clone(),
                acked_deployment_ids: BTreeSet::new(),
                updated_at: state.updated_at,
            };
            state.pending_rotation = Some(pending.clone());
            Ok(StateCommit::Changed((pending, local_peer, previous_access_key)))
        })
        .await?;

        if !previous_access_key.is_empty()
            && let Ok(previous_iam_secret) = site_replicator_service_account_secret(&previous_access_key).await
        {
            record_pending_rotation_secret_candidate(&pending_rotation.id, previous_iam_secret).await?;
        }

        set_site_replicator_service_account_secret(&pending_rotation.parent, pending_rotation.new_secret_key.clone()).await?;

        refresh_bucket_targets_after_service_account_rotation().await;

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
                &runtime_peer_connection(peer)?,
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
    use super::*;
    use crate::admin::runtime_sources::{current_outbound_tls_generation, set_test_outbound_tls_generation};
    use crate::admin::storage_api::runtime::Endpoint;
    use crate::admin::storage_api::runtime::{EndpointServerPools, Endpoints, PoolEndpoints};
    use axum::{Router, extract::State, routing::any};
    use http::{HeaderMap, HeaderValue, Uri};
    use rustfs_policy::policy::action::S3Action;
    use serial_test::serial;
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    };
    use temp_env::with_var;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[test]
    fn sts_replication_restores_groups_from_signed_claims() {
        let claims = HashMap::from([("groups".to_string(), serde_json::json!(["devs", "auditors"]))]);

        assert_eq!(
            string_list_claim(&claims, "groups"),
            Some(vec!["devs".to_string(), "auditors".to_string()])
        );
    }

    #[test]
    fn oidc_sts_replication_uses_signed_policy_instead_of_virtual_parent_mapping() {
        let verified_claims =
            HashMap::from([(OIDC_VIRTUAL_PARENT_CLAIM.to_string(), Value::String("openid=parent".to_string()))]);
        let legacy_claims = HashMap::new();

        assert!(sts_replication_compatibility_policy(&verified_claims, "readonly").is_none());
        assert_eq!(sts_replication_compatibility_policy(&legacy_claims, "readonly"), Some("readonly"));
    }

    /// Publish a ready IAM app context so `apply_iam_item` gets past its IAM guard.
    async fn publish_ready_iam_context() {
        use crate::admin::runtime_sources::{AppContext, publish_test_app_context};
        use rustfs_iam::store::{Store as _, object::IAM_CONFIG_PREFIX};

        let _ = rustfs_credentials::init_global_action_credentials(
            Some("TESTROOTACCESSKEY".to_string()),
            Some("TESTROOTSECRET123".to_string()),
        );
        if current_iam_handle().is_none() {
            let env = rustfs_test_utils::TestECStoreEnv::builder()
                .prefix("site_replication_iam_item")
                .disk_count(1)
                .init_bucket_metadata(false)
                .build()
                .await;
            rustfs_iam::store::object::ObjectStore::new(Arc::clone(&env.ecstore))
                .save_iam_config(serde_json::json!({"version": 1}), format!("{}/format.json", *IAM_CONFIG_PREFIX))
                .await
                .expect("seed IAM format");
            let iam = rustfs_iam::build_iam_sys(Arc::clone(&env.ecstore))
                .await
                .expect("build test IAM");
            publish_test_app_context(Arc::new(AppContext::with_default_interfaces(
                env.ecstore,
                iam,
                Arc::new(rustfs_kms::KmsServiceManager::new()),
            )));
        }
        assert!(current_iam_handle().is_some(), "test IAM should be published");
    }

    fn replicated_sts_item(item_type: &str) -> SRIAMItem {
        SRIAMItem {
            r#type: item_type.to_string(),
            sts_credential: Some(rustfs_madmin::SRSTSCredential {
                access_key: "REPLICATEDSTSACCESS".to_string(),
                secret_key: "replicatedStsSecret123".to_string(),
                session_token: "not-a-valid-session-token".to_string(),
                parent_user: "replicated-sts-parent".to_string(),
                parent_policy_mapping: String::new(),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            }),
            updated_at: Some(OffsetDateTime::UNIX_EPOCH),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        }
    }

    #[tokio::test]
    #[serial]
    async fn apply_iam_item_accepts_minio_sts_account_item_type() {
        publish_ready_iam_context().await;

        // MinIO madmin-go sends `SRIAMItemSTSAcc = "sts-account"`. The bogus session token
        // must reach token verification — falling into the unknown-type NotImplemented arm
        // means MinIO-originated STS replication would be rejected.
        let err = apply_iam_item(replicated_sts_item("sts-account"))
            .await
            .expect_err("bogus session token must fail verification");
        assert_ne!(
            *err.code(),
            S3ErrorCode::NotImplemented,
            "sts-account must be dispatched to the STS credential arm, got: {err:?}"
        );
        assert!(
            err.message().unwrap_or_default().contains("invalid STS session token"),
            "expected a token verification error, got: {err:?}"
        );
    }

    #[tokio::test]
    #[serial]
    async fn apply_iam_item_still_accepts_legacy_sts_credential_item_type() {
        publish_ready_iam_context().await;

        // Older RustFS peers emit `sts-credential`; the alias stays accepted permanently
        // so mixed-version RustFS sites keep replicating STS credentials.
        let err = apply_iam_item(replicated_sts_item("sts-credential"))
            .await
            .expect_err("bogus session token must fail verification");
        assert_ne!(
            *err.code(),
            S3ErrorCode::NotImplemented,
            "legacy sts-credential must stay accepted, got: {err:?}"
        );
        assert!(
            err.message().unwrap_or_default().contains("invalid STS session token"),
            "expected a token verification error, got: {err:?}"
        );
    }

    #[test]
    fn oidc_service_account_envelope_round_trips_actual_policy() {
        let actual_policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/*"]}]}"#;
        let updated_at = OffsetDateTime::UNIX_EPOCH;
        let claims =
            HashMap::from([(OIDC_VIRTUAL_PARENT_CLAIM.to_string(), Value::String("openid=verified-parent".to_string()))]);
        let (wire_policy, envelope) =
            encode_service_account_replication_policy(&claims, Some(actual_policy)).expect("encode envelope");
        let create = SRSvcAccCreate {
            parent: "openid=verified-parent".to_string(),
            access_key: "OIDCREPLICATEDSERVICE".to_string(),
            secret_key: "oidcReplicatedSecret123".to_string(),
            groups: Vec::new(),
            claims,
            session_policy: wire_policy,
            status: String::new(),
            name: String::new(),
            description: String::new(),
            expiration: None,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        };
        let old_receiver_policy: Policy = serde_json::from_str(
            create
                .session_policy
                .as_str()
                .expect("old receiver gets a standard session policy"),
        )
        .expect("parse old receiver policy");
        assert_eq!(
            serde_json::to_value(old_receiver_policy).expect("serialize old receiver policy"),
            serde_json::from_str::<Value>(actual_policy).expect("parse expected policy")
        );
        assert_eq!(envelope.as_ref().map(|envelope| envelope.version), Some(SERVICE_ACCOUNT_ENVELOPE_VERSION));
        assert_eq!(create.claims.len(), 1);

        let decoded = decode_service_account_replication_policy(&create, envelope.as_ref(), Some(updated_at), None)
            .expect("decode envelope")
            .expect("current envelope");
        assert!(decoded.is_envelope);
        let restored = decoded.policy.expect("actual policy");

        assert_eq!(
            serde_json::to_value(restored).expect("serialize restored policy"),
            serde_json::from_str::<Value>(actual_policy).expect("parse expected policy")
        );
    }

    #[test]
    fn oidc_service_account_envelope_clears_policy_on_existing_account() {
        let updated_at = OffsetDateTime::UNIX_EPOCH;
        let claims =
            HashMap::from([(OIDC_VIRTUAL_PARENT_CLAIM.to_string(), Value::String("openid=verified-parent".to_string()))]);
        let (wire_policy, envelope) =
            encode_service_account_replication_policy(&claims, None).expect("encode inherited envelope");
        let create = SRSvcAccCreate {
            parent: "openid=verified-parent".to_string(),
            access_key: "OIDCREPLICATEDSERVICE".to_string(),
            secret_key: "oidcReplicatedSecret123".to_string(),
            groups: Vec::new(),
            claims,
            session_policy: wire_policy,
            status: String::new(),
            name: String::new(),
            description: String::new(),
            expiration: None,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        };
        let old_receiver_policy: Policy = serde_json::from_str(
            create
                .session_policy
                .as_str()
                .expect("old receiver gets an explicit empty policy"),
        )
        .expect("parse old receiver policy");
        assert!(old_receiver_policy.version.is_empty());
        assert!(old_receiver_policy.statements.is_empty());

        let decoded = decode_service_account_replication_policy(&create, envelope.as_ref(), Some(updated_at), None)
            .expect("decode inherited envelope")
            .expect("current envelope");

        assert!(decoded.is_envelope);
        assert!(decoded.policy.is_none());
        assert_eq!(decoded.metadata_for_existing_account(String::new()), Some(String::new()));
        let update_policy = decoded
            .for_existing_account()
            .expect("existing account needs an explicit clear");
        assert!(update_policy.version.is_empty());
        assert!(update_policy.statements.is_empty());
    }

    #[test]
    fn oidc_service_account_envelope_replays_normalized_empty_policy() {
        let actual_policy = r#"{"ID":"deny-boundary","Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":["s3:*"],"Resource":["arn:aws:s3:::*"]}]}"#;
        let claims =
            HashMap::from([(OIDC_VIRTUAL_PARENT_CLAIM.to_string(), Value::String("openid=verified-parent".to_string()))]);
        let (wire_policy, envelope) =
            encode_service_account_replication_policy(&claims, Some(actual_policy)).expect("encode envelope");
        let create = SRSvcAccCreate {
            parent: "openid=verified-parent".to_string(),
            access_key: "OIDCREPLICATEDSERVICE".to_string(),
            secret_key: "oidcReplicatedSecret123".to_string(),
            groups: Vec::new(),
            claims,
            session_policy: wire_policy,
            status: "on".to_string(),
            name: String::new(),
            description: String::new(),
            expiration: None,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        };

        let decoded =
            decode_service_account_replication_policy(&create, envelope.as_ref(), Some(OffsetDateTime::UNIX_EPOCH), None)
                .expect("decode normalized empty policy")
                .expect("current envelope");
        let restored = decoded.policy.as_ref().expect("normalized policy must remain explicit");
        assert_eq!(
            serde_json::to_value(restored).expect("serialize restored policy"),
            serde_json::from_str::<Value>(actual_policy).expect("parse expected policy")
        );
        assert!(decoded.for_existing_account().is_some());
    }

    #[test]
    fn oidc_service_account_envelope_rejects_missing_policy() {
        let create = SRSvcAccCreate {
            parent: "openid=verified-parent".to_string(),
            access_key: "OIDCREPLICATEDSERVICE".to_string(),
            secret_key: "oidcReplicatedSecret123".to_string(),
            groups: Vec::new(),
            claims: HashMap::from([(OIDC_VIRTUAL_PARENT_CLAIM.to_string(), Value::String("openid=verified-parent".to_string()))]),
            session_policy: SRSessionPolicy::default(),
            status: String::new(),
            name: String::new(),
            description: String::new(),
            expiration: None,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        };

        let envelope = rustfs_madmin::SRSvcAccReplicationEnvelope {
            version: SERVICE_ACCOUNT_ENVELOPE_VERSION,
        };
        let err = decode_service_account_replication_policy(&create, Some(&envelope), Some(OffsetDateTime::UNIX_EPOCH), None)
            .expect_err("policy-less envelope must fail closed");

        assert_eq!(*err.code(), S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn stale_oidc_service_account_envelope_is_ignored_before_decoding() {
        let create = SRSvcAccCreate {
            parent: "openid=verified-parent".to_string(),
            access_key: "OIDCREPLICATEDSERVICE".to_string(),
            secret_key: "oidcReplicatedSecret123".to_string(),
            groups: Vec::new(),
            claims: HashMap::new(),
            session_policy: SRSessionPolicy::default(),
            status: String::new(),
            name: String::new(),
            description: String::new(),
            expiration: None,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        };

        let envelope = rustfs_madmin::SRSvcAccReplicationEnvelope {
            version: SERVICE_ACCOUNT_ENVELOPE_VERSION + 1,
        };
        let decoded = decode_service_account_replication_policy(
            &create,
            Some(&envelope),
            Some(OffsetDateTime::UNIX_EPOCH),
            Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(1)),
        )
        .expect("stale envelope must be ignored before validation");

        assert!(decoded.is_none());
    }

    #[test]
    fn oidc_service_account_envelope_does_not_survive_a_legacy_hop() {
        #[derive(serde::Deserialize, serde::Serialize)]
        struct LegacyServiceAccountChange {
            #[serde(rename = "crSvcAccCreate", skip_serializing_if = "Option::is_none")]
            create: Option<SRSvcAccCreate>,
            #[serde(rename = "apiVersion", skip_serializing_if = "Option::is_none")]
            api_version: Option<String>,
        }

        let claims =
            HashMap::from([(OIDC_VIRTUAL_PARENT_CLAIM.to_string(), Value::String("openid=verified-parent".to_string()))]);
        let (session_policy, envelope) =
            encode_service_account_replication_policy(&claims, None).expect("encode envelope for legacy hop");
        let change = rustfs_madmin::SRSvcAccChange {
            create: Some(SRSvcAccCreate {
                parent: "openid=verified-parent".to_string(),
                access_key: "OIDCREPLICATEDSERVICE".to_string(),
                secret_key: "oidcReplicatedSecret123".to_string(),
                groups: Vec::new(),
                claims,
                session_policy,
                status: String::new(),
                name: String::new(),
                description: String::new(),
                expiration: None,
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            }),
            oidc_service_account_envelope: envelope,
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
            ..Default::default()
        };

        let legacy: LegacyServiceAccountChange =
            serde_json::from_value(serde_json::to_value(change).expect("serialize new replication payload"))
                .expect("legacy node must ignore the unknown envelope field");
        let legacy_claims = legacy
            .create
            .as_ref()
            .expect("legacy payload has a create operation")
            .claims
            .clone();
        assert_eq!(legacy_claims.len(), 1);

        let reemitted: rustfs_madmin::SRSvcAccChange = serde_json::from_value(
            serde_json::to_value(LegacyServiceAccountChange {
                create: Some(SRSvcAccCreate {
                    parent: "openid=verified-parent".to_string(),
                    access_key: "OIDCLEGACYCHILD001".to_string(),
                    secret_key: "oidcLegacyChildSecret123".to_string(),
                    groups: Vec::new(),
                    claims: legacy_claims,
                    session_policy: SRSessionPolicy::default(),
                    status: String::new(),
                    name: String::new(),
                    description: String::new(),
                    expiration: None,
                    api_version: Some(SITE_REPL_API_VERSION.to_string()),
                }),
                api_version: Some(SITE_REPL_API_VERSION.to_string()),
            })
            .expect("serialize legacy child replication payload"),
        )
        .expect("new node accepts legacy child replication payload");

        assert!(reemitted.oidc_service_account_envelope.is_none());
        let create = reemitted.create.expect("reemitted payload has a create operation");
        let decoded = decode_service_account_replication_policy(&create, None, Some(OffsetDateTime::UNIX_EPOCH), None)
            .expect("legacy payload must not be parsed as an envelope")
            .expect("legacy payload should be accepted");
        assert!(!decoded.is_envelope);
    }

    fn valid_test_ca_pem(name: &str) -> String {
        rcgen::generate_simple_self_signed(vec![name.to_string()])
            .expect("generate test CA")
            .cert
            .pem()
    }

    fn empty_outbound_tls_state() -> GlobalPublishedOutboundTlsState {
        GlobalPublishedOutboundTlsState {
            generation: rustfs_tls_runtime::TlsGeneration(0),
            root_ca_pem: None,
            mtls_identity: None,
        }
    }

    struct TestTlsIdentity {
        cert_pem: String,
        cert_der: rustls_pki_types::CertificateDer<'static>,
        key_der: rustls_pki_types::PrivateKeyDer<'static>,
    }

    fn test_tls_identity() -> TestTlsIdentity {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let certified =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).expect("generate TLS server certificate");
        TestTlsIdentity {
            cert_pem: certified.cert.pem(),
            cert_der: certified.cert.der().clone(),
            key_der: rustls_pki_types::PrivateKeyDer::try_from(certified.signing_key.serialize_der())
                .expect("convert TLS server private key"),
        }
    }

    async fn spawn_recording_tls_server(
        identity: &TestTlsIdentity,
        response: &'static [u8],
    ) -> (String, tokio::task::JoinHandle<Option<String>>) {
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![identity.cert_der.clone()], identity.key_der.clone_key())
            .expect("build recording TLS server config");
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind recording TLS server");
        let endpoint = format!("https://{}", listener.local_addr().expect("recording TLS server address"));
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.ok()?;
            let mut stream = acceptor.accept(stream).await.ok()?;
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let read = stream.read(&mut buffer).await.ok()?;
                if read == 0 {
                    return None;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let method = std::str::from_utf8(&request).ok()?.split_whitespace().next()?.to_string();
            stream.write_all(response).await.ok()?;
            Some(method)
        });
        (endpoint, task)
    }

    async fn spawn_test_tls_server() -> (String, String, tokio::task::JoinHandle<bool>) {
        spawn_test_tls_server_with_response(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok").await
    }

    async fn spawn_test_tls_server_with_response(response: &'static [u8]) -> (String, String, tokio::task::JoinHandle<bool>) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let certified =
            rcgen::generate_simple_self_signed(vec!["127.0.0.1".to_string()]).expect("generate TLS server certificate");
        let ca_pem = certified.cert.pem();
        let private_key = rustls_pki_types::PrivateKeyDer::try_from(certified.signing_key.serialize_der())
            .expect("convert TLS server private key");
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certified.cert.der().clone()], private_key)
            .expect("build TLS server config");
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind TLS test server");
        let endpoint = format!("https://{}", listener.local_addr().expect("TLS test server address"));
        let task = tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return false;
            };
            let Ok(mut stream) = acceptor.accept(stream).await else {
                return false;
            };
            let mut request = Vec::new();
            let mut buffer = [0_u8; 1024];
            loop {
                let Ok(read) = stream.read(&mut buffer).await else {
                    return false;
                };
                if read == 0 {
                    return false;
                }
                request.extend_from_slice(&buffer[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            stream.write_all(response).await.is_ok()
        });
        (endpoint, ca_pem, task)
    }

    #[test]
    fn peer_connection_validation_accepts_supported_combinations() {
        let ca = valid_test_ca_pem("peer.example.com");

        assert!(validate_peer_connection_inner("http://10.0.0.5:9000", false, "", false).is_ok());
        assert!(validate_peer_connection_inner("https://peer.example.com", false, "", false).is_ok());
        assert!(validate_peer_connection_inner("https://peer.example.com", true, "", false).is_ok());
        assert!(validate_peer_connection_inner("https://peer.example.com", false, &ca, false).is_ok());
    }

    #[test]
    fn peer_connection_validation_rejects_invalid_tls_combinations() {
        let ca = valid_test_ca_pem("peer.example.com");

        for (endpoint, skip_tls_verify, ca_cert_pem) in [
            ("http://10.0.0.5:9000", true, ""),
            ("http://10.0.0.5:9000", false, ca.as_str()),
            ("https://peer.example.com", true, ca.as_str()),
        ] {
            assert!(validate_peer_connection_inner(endpoint, skip_tls_verify, ca_cert_pem, false).is_err());
        }
    }

    #[test]
    fn peer_connection_validation_requires_pure_origin() {
        for endpoint in [
            "ftp://peer.example.com",
            "https://user@peer.example.com",
            "https://peer.example.com/admin",
            "https://peer.example.com/?query=1",
            "https://peer.example.com/#fragment",
        ] {
            assert!(
                validate_peer_connection_inner(endpoint, false, "", false).is_err(),
                "endpoint should be rejected: {endpoint}"
            );
        }
        assert!(validate_peer_connection_inner("https://peer.example.com/", false, "", false).is_ok());
    }

    #[test]
    fn peer_connection_validation_matches_replication_egress_policy() {
        assert!(validate_peer_connection_inner("http://10.0.0.5:9000", false, "", false).is_ok());
        assert!(validate_peer_connection_inner("http://127.0.0.1:9000", false, "", false).is_err());
        assert!(validate_peer_connection_inner("http://127.0.0.1:9000", false, "", true).is_ok());
        assert!(validate_peer_connection_inner("http://[::1]:9000", false, "", true).is_ok());
        assert!(validate_peer_connection_inner("http://localhost:9000", false, "", true).is_ok());

        for endpoint in [
            "http://169.254.169.254",
            "http://[fe80::1]:9000",
            "http://0.0.0.0:9000",
            "http://[::ffff:127.0.0.1]:9000",
            "http://[::127.0.0.1]:9000",
            "http://[::ffff:169.254.169.254]:9000",
        ] {
            assert!(
                validate_peer_connection_inner(endpoint, false, "", true).is_err(),
                "endpoint should remain forbidden with loopback opt-in: {endpoint}"
            );
        }
    }

    #[test]
    fn peer_connection_validation_accepts_multi_cert_ca_and_rejects_unsafe_pem() {
        let multi_cert = format!("{}{}", valid_test_ca_pem("one.example.com"), valid_test_ca_pem("two.example.com"));
        assert!(validate_peer_connection_inner("https://peer.example.com", false, &multi_cert, false).is_ok());

        for pem in [
            "not a certificate",
            "-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----",
            "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----",
            "-----BEGIN RSA PRIVATE KEY-----\nsecret\n-----END RSA PRIVATE KEY-----",
        ] {
            assert!(validate_peer_connection_inner("https://peer.example.com", false, pem, false).is_err());
        }

        let oversized = "x".repeat(MAX_PEER_CA_CERT_PEM_SIZE + 1);
        assert!(validate_peer_connection_inner("https://peer.example.com", false, &oversized, false).is_err());
    }

    #[test]
    fn persisted_peer_connection_errors_are_internal_and_refresh_can_use_valid_candidate() {
        let invalid_peer = PeerInfo {
            endpoint: "https://peer.example.com/not-an-origin".to_string(),
            deployment_id: "remote".to_string(),
            ..Default::default()
        };
        let runtime_error = runtime_peer_connection(&invalid_peer).expect_err("invalid persisted peer must fail");
        assert_eq!(runtime_error.code(), &S3ErrorCode::InternalError);

        let input_site = PeerSite {
            endpoint: invalid_peer.endpoint.clone(),
            ..Default::default()
        };
        let input_error = PeerConnection::try_from(&input_site).expect_err("invalid input site must fail");
        assert_eq!(input_error.code(), &S3ErrorCode::InvalidRequest);

        let pending = PendingEndpointRefresh {
            peer: PeerInfo {
                endpoint: "https://replacement.example.com".to_string(),
                deployment_id: "remote".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let candidates = endpoint_refresh_route_endpoints(&invalid_peer, &pending)
            .expect("valid replacement endpoint must survive invalid persisted endpoint");
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].endpoint(), "https://replacement.example.com");
    }

    #[tokio::test]
    async fn peer_dns_resolver_filters_forbidden_addresses_and_reqwest_cannot_bypass() {
        let resolver = PeerDnsResolver::with_overrides(
            true,
            HashMap::from([
                ("public.test".to_string(), vec!["8.8.8.8".parse().expect("public IP")]),
                ("private.test".to_string(), vec!["10.0.0.5".parse().expect("private IP")]),
                ("metadata.test".to_string(), vec!["169.254.169.254".parse().expect("metadata IP")]),
                ("alias.test".to_string(), vec!["127.0.0.1".parse().expect("loopback IP")]),
                ("mapped.test".to_string(), vec!["::ffff:127.0.0.1".parse().expect("mapped loopback IP")]),
                ("localhost".to_string(), vec!["127.0.0.1".parse().expect("localhost IP")]),
            ]),
        );

        for host in ["public.test", "private.test", "localhost"] {
            let address_count = reqwest::dns::Resolve::resolve(&resolver, host.parse().expect("resolver test hostname"))
                .await
                .expect("allowed resolver result")
                .count();
            assert_eq!(address_count, 1, "expected one allowed address for {host}");
        }
        for host in ["metadata.test", "alias.test", "mapped.test"] {
            assert!(
                reqwest::dns::Resolve::resolve(&resolver, host.parse().expect("resolver test hostname"))
                    .await
                    .is_err(),
                "resolver must reject {host}"
            );
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind resolver bypass listener");
        let port = listener.local_addr().expect("resolver bypass listener address").port();
        let accepted = Arc::new(AtomicBool::new(false));
        let accepted_by_server = accepted.clone();
        let server = tokio::spawn(async move {
            if listener.accept().await.is_ok() {
                accepted_by_server.store(true, Ordering::SeqCst);
            }
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .dns_resolver(resolver)
            .build()
            .expect("resolver bypass client");
        assert!(client.get(format!("http://alias.test:{port}/")).send().await.is_err());
        assert!(!accepted.load(Ordering::SeqCst));
        server.abort();
    }

    #[tokio::test]
    #[serial]
    async fn production_peer_clients_ignore_environment_proxies_before_dns_filtering() {
        let proxy_listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind observable proxy listener");
        let proxy_url = format!("http://{}", proxy_listener.local_addr().expect("observable proxy listener address"));
        let (proxy_hit_tx, mut proxy_hit_rx) = tokio::sync::mpsc::unbounded_channel();
        let proxy = tokio::spawn(async move {
            while let Ok((_stream, _address)) = proxy_listener.accept().await {
                if proxy_hit_tx.send(()).is_err() {
                    break;
                }
            }
        });

        temp_env::async_with_vars(
            [
                ("HTTP_PROXY", Some(proxy_url.as_str())),
                ("HTTPS_PROXY", Some(proxy_url.as_str())),
                ("ALL_PROXY", Some(proxy_url.as_str())),
                ("http_proxy", Some(proxy_url.as_str())),
                ("https_proxy", Some(proxy_url.as_str())),
                ("all_proxy", Some(proxy_url.as_str())),
                ("NO_PROXY", Some("")),
                ("no_proxy", Some("")),
            ],
            async {
                let resolver = PeerDnsResolver::with_overrides(
                    false,
                    HashMap::from([("metadata.test".to_string(), vec!["169.254.169.254".parse().expect("metadata IP")])]),
                );
                let outbound_tls = empty_outbound_tls_state();
                let default_connection =
                    validate_peer_connection_inner("http://metadata.test", false, "", false).expect("default peer connection");
                let custom_connection =
                    validate_peer_connection_inner("https://metadata.test", true, "", false).expect("custom peer connection");
                let default_client = build_site_replication_peer_client_with_resolver(&outbound_tls, resolver.clone())
                    .expect("default production peer client");
                let custom_client =
                    build_custom_site_replication_peer_client_with_resolver(&outbound_tls, &custom_connection, resolver)
                        .expect("custom production peer client");

                for (client, connection) in [(&default_client, &default_connection), (&custom_client, &custom_connection)] {
                    let result = send_peer_admin_get_request_with_client(
                        client,
                        connection,
                        "/rustfs/admin/v3/site-replication/metainfo",
                        "access-key",
                        "secret-key",
                    )
                    .await;
                    assert!(result.is_err(), "forbidden DNS result must fail closed");
                }
            },
        )
        .await;

        assert!(
            tokio::time::timeout(Duration::from_millis(100), proxy_hit_rx.recv())
                .await
                .is_err(),
            "site-replication peer traffic must never reach an environment proxy"
        );
        proxy.abort();
    }

    #[test]
    fn peer_url_join_preserves_wire_path_and_query_encoding() {
        let connection =
            validate_peer_connection_inner("https://peer.example.com", false, "", false).expect("peer connection for URL join");
        let url = site_replication_peer_url(
            &connection,
            "/minio/admin/v3/site-replication/peer/bucket-ops?bucket=a%2Fb&operation=configure-replication",
        )
        .expect("join peer wire URL");

        assert_eq!(
            url.as_str(),
            "https://peer.example.com/minio/admin/v3/site-replication/peer/bucket-ops?bucket=a%2Fb&operation=configure-replication"
        );
    }

    #[tokio::test]
    async fn peer_clients_isolate_skip_and_custom_ca_trust() {
        let outbound_tls = empty_outbound_tls_state();

        let (ca_endpoint, ca_pem, ca_server) = spawn_test_tls_server().await;
        let ca_connection =
            validate_peer_connection_inner(&ca_endpoint, false, &ca_pem, true).expect("custom CA peer connection");
        let ca_client = build_custom_site_replication_peer_client(&outbound_tls, &ca_connection).expect("custom CA peer client");
        assert_eq!(
            ca_client.get(&ca_endpoint).send().await.expect("custom CA request").status(),
            StatusCode::OK
        );
        assert!(ca_server.await.expect("custom CA server task"));

        let (untrusted_endpoint, _untrusted_ca, untrusted_server) = spawn_test_tls_server().await;
        assert!(ca_client.get(&untrusted_endpoint).send().await.is_err());
        assert!(!untrusted_server.await.expect("untrusted TLS server task"));

        let (other_endpoint, other_ca, other_server) = spawn_test_tls_server().await;
        let other_connection =
            validate_peer_connection_inner(&other_endpoint, false, &other_ca, true).expect("second custom CA peer connection");
        let other_client =
            build_custom_site_replication_peer_client(&outbound_tls, &other_connection).expect("second custom CA peer client");
        assert_eq!(
            other_client
                .get(&other_endpoint)
                .send()
                .await
                .expect("second custom CA request")
                .status(),
            StatusCode::OK
        );
        assert!(other_server.await.expect("second custom CA server task"));

        let (skip_endpoint, _skip_ca, skip_server) = spawn_test_tls_server().await;
        let skip_connection =
            validate_peer_connection_inner(&skip_endpoint, true, "", true).expect("skip-verify peer connection");
        let skip_client =
            build_custom_site_replication_peer_client(&outbound_tls, &skip_connection).expect("skip-verify peer client");
        assert_eq!(
            skip_client
                .get(&skip_endpoint)
                .send()
                .await
                .expect("skip-verify request")
                .status(),
            StatusCode::OK
        );
        assert!(skip_server.await.expect("skip-verify server task"));
    }

    #[tokio::test]
    #[serial]
    async fn peer_admin_transport_uses_full_connection_for_get_and_put() {
        temp_env::async_with_vars([(ALLOW_LOOPBACK_REPLICATION_TARGET_ENV, Some("true"))], async {
            let ca_identity = test_tls_identity();
            let (ca_endpoint, ca_server) =
                spawn_recording_tls_server(&ca_identity, b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok")
                    .await;
            let ca_connection =
                PeerConnection::new(&ca_endpoint, false, &ca_identity.cert_pem).expect("production custom-CA peer connection");
            let get_body = send_peer_admin_get_request(&ca_connection, "/rustfs/admin/v3/site-replication/metainfo", "ak", "sk")
                .await
                .expect("production custom-CA GET");
            assert_eq!(get_body, b"ok");
            assert_eq!(ca_server.await.expect("custom-CA GET server task").as_deref(), Some("GET"));

            let skip_identity = test_tls_identity();
            let (skip_endpoint, skip_server) = spawn_recording_tls_server(
                &skip_identity,
                b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok",
            )
            .await;
            let skip_connection = PeerConnection::new(&skip_endpoint, true, "").expect("production skip-verify peer connection");
            let (status, put_body) = send_peer_admin_request_raw(
                &skip_connection,
                "/rustfs/admin/v3/site-replication/peer/edit",
                "ak",
                "sk",
                &serde_json::json!({"peer": "test"}),
            )
            .await
            .expect("production skip-verify PUT");
            assert_eq!(status, StatusCode::OK);
            assert_eq!(put_body, b"ok");
            assert_eq!(skip_server.await.expect("skip-verify PUT server task").as_deref(), Some("PUT"));
        })
        .await;
    }

    #[tokio::test]
    async fn custom_peer_client_composes_global_and_peer_roots_without_leaking_peer_root() {
        let global_identity = test_tls_identity();
        let peer_identity = test_tls_identity();
        let unrelated_identity = test_tls_identity();
        let outbound_tls = GlobalPublishedOutboundTlsState {
            generation: rustfs_tls_runtime::TlsGeneration(1),
            root_ca_pem: Some(global_identity.cert_pem.as_bytes().to_vec()),
            mtls_identity: None,
        };

        let (peer_endpoint, peer_server) =
            spawn_recording_tls_server(&peer_identity, b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .await;
        let peer_connection =
            validate_peer_connection_inner(&peer_endpoint, false, &peer_identity.cert_pem, true).expect("peer-root connection");
        let peer_client =
            build_custom_site_replication_peer_client(&outbound_tls, &peer_connection).expect("composed peer client");
        assert_eq!(
            peer_client
                .get(&peer_endpoint)
                .send()
                .await
                .expect("peer-root request")
                .status(),
            StatusCode::OK
        );
        assert_eq!(peer_server.await.expect("peer-root server task").as_deref(), Some("GET"));

        let (global_endpoint, global_server) =
            spawn_recording_tls_server(&global_identity, b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .await;
        assert_eq!(
            peer_client
                .get(&global_endpoint)
                .send()
                .await
                .expect("global-root request through peer client")
                .status(),
            StatusCode::OK
        );
        assert_eq!(global_server.await.expect("global-root server task").as_deref(), Some("GET"));

        let unrelated_connection =
            validate_peer_connection_inner("https://127.0.0.1:1", false, &unrelated_identity.cert_pem, true)
                .expect("unrelated peer connection");
        let unrelated_client =
            build_custom_site_replication_peer_client(&outbound_tls, &unrelated_connection).expect("unrelated peer client");
        let (peer_endpoint, peer_server) =
            spawn_recording_tls_server(&peer_identity, b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .await;
        assert!(unrelated_client.get(&peer_endpoint).send().await.is_err());
        assert!(peer_server.await.expect("unrelated peer isolation server task").is_none());
    }

    #[tokio::test]
    async fn peer_clients_do_not_follow_redirects() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind redirect test server");
        let endpoint = format!("http://{}", listener.local_addr().expect("redirect test server address"));
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept redirect test request");
            let mut request = [0_u8; 1024];
            let read = stream.read(&mut request).await.expect("read redirect test request");
            assert!(read > 0);
            stream
                .write_all(b"HTTP/1.1 302 Found\r\nlocation: /followed\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                .await
                .expect("write redirect response");
        });

        let client = build_site_replication_peer_client(&empty_outbound_tls_state()).expect("default peer client");
        let response = client.get(&endpoint).send().await.expect("redirect test request");
        assert_eq!(response.status(), StatusCode::FOUND);
        server.await.expect("redirect test server task");

        let (tls_endpoint, _tls_ca, tls_server) = spawn_test_tls_server_with_response(
            b"HTTP/1.1 302 Found\r\nlocation: /followed\r\ncontent-length: 0\r\nconnection: close\r\n\r\n",
        )
        .await;
        let connection = validate_peer_connection_inner(&tls_endpoint, true, "", true).expect("custom redirect peer connection");
        let client = build_custom_site_replication_peer_client(&empty_outbound_tls_state(), &connection)
            .expect("custom redirect peer client");
        let response = client.get(&tls_endpoint).send().await.expect("custom redirect test request");
        assert_eq!(response.status(), StatusCode::FOUND);
        assert!(tls_server.await.expect("custom redirect TLS server task"));
    }

    fn peer(name: &str, endpoint: &str) -> PeerInfo {
        PeerInfo {
            name: name.to_string(),
            endpoint: endpoint.to_string(),
            deployment_id: String::new(),
            sync_state: SyncStatus::Unknown,
            default_bandwidth: BucketBandwidth::default(),
            replicate_ilm_expiry: false,
            object_naming_mode: String::new(),
            skip_tls_verify: false,
            ca_cert_pem: String::new(),
            api_version: Some(SITE_REPL_API_VERSION.to_string()),
        }
    }

    #[test]
    fn test_stored_peer_tls_settings_preserve_configured_values() {
        let stored_peer = PeerInfo {
            skip_tls_verify: true,
            ca_cert_pem: "custom-ca".to_string(),
            ..peer("local", "https://local.example.com")
        };

        assert_eq!(stored_peer_tls_settings(Some(&stored_peer)), (true, "custom-ca".to_string()));
        assert_eq!(stored_peer_tls_settings(None), (false, String::new()));
    }

    #[test]
    fn test_normalize_peer_site_preserves_tls_settings() {
        let peer = normalize_peer_site(
            PeerSite {
                name: "remote".to_string(),
                endpoint: "https://remote.example.com".to_string(),
                skip_tls_verify: true,
                ca_cert_pem: "custom-ca".to_string(),
                ..PeerSite::default()
            },
            false,
        );

        assert!(peer.skip_tls_verify);
        assert_eq!(peer.ca_cert_pem, "custom-ca");
    }

    #[test]
    fn test_build_join_peers_applies_local_site_tls_settings() {
        let local_peer = PeerInfo {
            deployment_id: "local-deployment".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let peers = build_join_peers(
            &SiteReplicationState::default(),
            &local_peer,
            vec![PeerSite {
                name: "local".to_string(),
                endpoint: "https://local.example.com/".to_string(),
                skip_tls_verify: true,
                ca_cert_pem: "local-ca".to_string(),
                ..PeerSite::default()
            }],
            false,
        );

        let local = peers.get("local-deployment").expect("local peer should be present");
        assert!(local.skip_tls_verify);
        assert_eq!(local.ca_cert_pem, "local-ca");
    }

    #[test]
    fn test_build_join_peers_prefers_explicit_https_for_all_local_peer_tls_modes() {
        let local_peer = PeerInfo {
            deployment_id: "local-deployment".to_string(),
            ..peer("local", "http://local.example.com:9000")
        };
        let custom_ca = valid_test_ca_pem("local.example.com");

        for (skip_tls_verify, ca_cert_pem) in [(false, String::new()), (true, String::new()), (false, custom_ca)] {
            let peers = build_join_peers(
                &SiteReplicationState::default(),
                &local_peer,
                vec![PeerSite {
                    name: "local".to_string(),
                    endpoint: "https://local.example.com:9000".to_string(),
                    skip_tls_verify,
                    ca_cert_pem: ca_cert_pem.clone(),
                    ..PeerSite::default()
                }],
                false,
            );

            let local = peers.get("local-deployment").expect("local peer should be present");
            assert_eq!(local.endpoint, "https://local.example.com:9000");
            assert_eq!(local.skip_tls_verify, skip_tls_verify);
            assert_eq!(local.ca_cert_pem, ca_cert_pem);
            assert!(validate_join_peer_snapshot(&peers).is_ok());
        }
    }

    #[test]
    fn test_build_join_peers_does_not_downgrade_local_https_tls_modes() {
        let custom_ca = valid_test_ca_pem("local.example.com");

        for (skip_tls_verify, ca_cert_pem) in [(false, String::new()), (true, String::new()), (false, custom_ca)] {
            let local_peer = PeerInfo {
                deployment_id: "local-deployment".to_string(),
                skip_tls_verify,
                ca_cert_pem: ca_cert_pem.clone(),
                ..peer("local", "https://local.example.com:9000")
            };
            let peers = build_join_peers(
                &SiteReplicationState::default(),
                &local_peer,
                vec![PeerSite {
                    name: "local".to_string(),
                    endpoint: "http://local.example.com:9000".to_string(),
                    ..PeerSite::default()
                }],
                false,
            );

            let local = peers.get("local-deployment").expect("local peer should be present");
            assert_eq!(local.endpoint, "https://local.example.com:9000");
            assert_eq!(local.skip_tls_verify, skip_tls_verify);
            assert_eq!(local.ca_cert_pem, ca_cert_pem);
            assert!(validate_join_peer_snapshot(&peers).is_ok());
        }
    }

    #[test]
    fn test_build_join_peers_explicitly_disables_existing_remote_tls_settings() {
        let local_peer = PeerInfo {
            deployment_id: "local-deployment".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let existing_remote = PeerInfo {
            deployment_id: "remote-deployment".to_string(),
            skip_tls_verify: true,
            ca_cert_pem: "old-remote-ca".to_string(),
            ..peer("remote", "https://remote.example.com")
        };
        let state = SiteReplicationState {
            peers: BTreeMap::from([("remote-deployment".to_string(), existing_remote)]),
            ..SiteReplicationState::default()
        };

        let peers = build_join_peers(
            &state,
            &local_peer,
            vec![PeerSite {
                name: "remote".to_string(),
                endpoint: "https://remote.example.com".to_string(),
                skip_tls_verify: false,
                ca_cert_pem: String::new(),
                ..PeerSite::default()
            }],
            false,
        );

        let remote = peers
            .get("remote-deployment")
            .expect("existing remote peer should be present");
        assert!(!remote.skip_tls_verify);
        assert_eq!(remote.ca_cert_pem, "");
    }

    #[test]
    fn test_public_peer_edit_missing_tls_fields_preserves_existing_settings() {
        let existing = PeerInfo {
            deployment_id: "remote-deployment".to_string(),
            skip_tls_verify: true,
            ..peer("remote", "https://remote.example.com")
        };
        let state = SiteReplicationState {
            peers: BTreeMap::from([("remote-deployment".to_string(), existing)]),
            ..Default::default()
        };
        let body = br#"{"deploymentID":"remote-deployment","endpoint":"https://remote.example.com","name":"renamed"}"#;
        let (mut incoming, presence) = parse_public_peer_edit(body).expect("parse public peer edit");

        apply_public_peer_edit_tls_presence(&state, &mut incoming, presence);

        assert!(incoming.skip_tls_verify);
        assert_eq!(incoming.ca_cert_pem, "");
    }

    #[test]
    fn test_public_peer_edit_explicit_default_tls_settings_are_propagated() {
        let existing = PeerInfo {
            deployment_id: "remote-deployment".to_string(),
            skip_tls_verify: true,
            ca_cert_pem: "old-ca".to_string(),
            ..peer("remote", "https://remote.example.com")
        };
        let state = SiteReplicationState {
            peers: BTreeMap::from([("remote-deployment".to_string(), existing)]),
            ..Default::default()
        };
        let body = br#"{"deploymentID":"remote-deployment","endpoint":"https://remote.example.com","skipTlsVerify":false,"caCertPem":""}"#;
        let (mut incoming, presence) = parse_public_peer_edit(body).expect("parse public peer edit");

        apply_public_peer_edit_tls_presence(&state, &mut incoming, presence);
        let propagated = serde_json::to_value(&incoming).expect("serialize propagated peer edit");

        assert!(!incoming.skip_tls_verify);
        assert_eq!(incoming.ca_cert_pem, "");
        assert_eq!(propagated.get("skipTlsVerify"), Some(&serde_json::json!(false)));
        assert_eq!(propagated.get("caCertPem"), Some(&serde_json::json!("")));
    }

    #[test]
    fn test_reconcile_join_response_preserves_requested_tls_trust() {
        let requested = PeerInfo {
            deployment_id: "temporary-id".to_string(),
            skip_tls_verify: true,
            ..peer("requested-name", "https://remote.example.com")
        };
        let state = SiteReplicationState {
            peers: BTreeMap::from([("temporary-id".to_string(), requested)]),
            ..Default::default()
        };

        let reconciled = reconcile_peer_with_actual_identity(
            state,
            PeerInfo {
                deployment_id: "actual-id".to_string(),
                api_version: Some("2".to_string()),
                ..peer("actual-name", "https://remote.example.com/")
            },
        );
        let actual = reconciled.peers.get("actual-id").expect("actual peer identity");

        assert_eq!(actual.name, "actual-name");
        assert_eq!(actual.api_version.as_deref(), Some("2"));
        assert!(actual.skip_tls_verify);
        assert_eq!(actual.ca_cert_pem, "");
    }

    #[test]
    fn test_internal_join_and_edit_reject_invalid_peer_tls_settings() {
        let invalid = PeerInfo {
            deployment_id: "remote".to_string(),
            skip_tls_verify: true,
            ..peer("remote", "http://remote.example.com")
        };

        assert!(validate_proposed_peer(&invalid).is_err());
        assert!(validate_join_peer_snapshot(&BTreeMap::from([("remote".to_string(), invalid)])).is_err());
    }

    #[test]
    fn test_internal_ilm_only_edit_does_not_create_a_pseudo_peer() {
        let local = PeerInfo {
            deployment_id: "local".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let remote = PeerInfo {
            deployment_id: "remote".to_string(),
            ..peer("remote", "https://remote.example.com")
        };
        let original_keys = BTreeSet::from(["local".to_string(), "remote".to_string()]);
        let state = SiteReplicationState {
            peers: BTreeMap::from([("local".to_string(), local.clone()), ("remote".to_string(), remote)]),
            ..Default::default()
        };

        let updated = apply_internal_peer_edit(state, &local, PeerInfo::default(), Some(true)).expect("ILM-only edit");

        assert_eq!(updated.peers.keys().cloned().collect::<BTreeSet<_>>(), original_keys);
        assert!(updated.peers.values().all(|peer| peer.replicate_ilm_expiry));
        assert!(!updated.peers.contains_key(&deployment_id_for_endpoint("")));
    }

    #[test]
    fn test_internal_empty_identity_edit_requires_only_an_ilm_override() {
        let local = PeerInfo {
            deployment_id: "local".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let state = SiteReplicationState {
            peers: BTreeMap::from([("local".to_string(), local.clone())]),
            ..Default::default()
        };

        assert!(apply_internal_peer_edit(state.clone(), &local, PeerInfo::default(), None).is_err());
        assert!(
            apply_internal_peer_edit(
                state,
                &local,
                PeerInfo {
                    skip_tls_verify: true,
                    ..Default::default()
                },
                Some(true),
            )
            .is_err()
        );
    }

    #[test]
    fn test_peer_tls_capability_gate_covers_full_topology_only_when_needed() {
        let default_sites = vec![PeerSite {
            endpoint: "https://remote.example.com".to_string(),
            ..Default::default()
        }];
        assert!(!add_peer_tls_capability_required(&default_sites));

        let custom_sites = vec![
            default_sites[0].clone(),
            PeerSite {
                endpoint: "https://custom.example.com".to_string(),
                skip_tls_verify: true,
                ..Default::default()
            },
        ];
        assert!(add_peer_tls_capability_required(&custom_sites));
        assert_eq!(peer_tls_capability_probe_sites(&custom_sites).len(), 2);

        let current = peer("remote", "https://remote.example.com");
        let changed = PeerInfo {
            skip_tls_verify: true,
            ..current.clone()
        };
        assert!(edit_peer_tls_capability_required(Some(&current), &changed));
        assert!(!edit_peer_tls_capability_required(Some(&changed), &changed));
        assert!(!edit_peer_tls_capability_required(Some(&changed), &current));
    }

    #[test]
    fn test_tls_only_edit_uses_pending_overlay_without_mutating_committed_peer() {
        let committed = PeerInfo {
            deployment_id: "remote".to_string(),
            ..peer("remote", "https://remote.example.com")
        };
        let proposed = PeerInfo {
            skip_tls_verify: true,
            ..committed.clone()
        };
        let state = SiteReplicationState {
            peers: BTreeMap::from([("remote".to_string(), committed)]),
            ..Default::default()
        };
        let pending = PendingEndpointRefresh {
            id: "refresh-tls".to_string(),
            peer: proposed,
            ..Default::default()
        };

        assert!(peer_endpoint_refresh_requested(&state, &pending.peer));
        let target_state = endpoint_refresh_target_state(&state, &pending);
        assert!(!state.peers["remote"].skip_tls_verify);
        assert!(target_state.peers["remote"].skip_tls_verify);
    }

    #[test]
    fn test_pending_endpoint_refresh_retry_summary_redacts_pem() {
        let pem = "-----BEGIN CERTIFICATE-----\nsecret-marker\n-----END CERTIFICATE-----";
        let mut state = SiteReplicationState::default();
        set_pending_endpoint_refresh(
            &mut state,
            PendingEndpointRefresh {
                id: "refresh-pem".to_string(),
                peer: PeerInfo {
                    deployment_id: "remote".to_string(),
                    ca_cert_pem: pem.to_string(),
                    ..peer("remote", "https://remote.example.com")
                },
                ..Default::default()
            },
        )
        .expect("set pending endpoint refresh");

        assert_eq!(
            state
                .pending_endpoint_refresh
                .as_ref()
                .expect("dedicated pending")
                .peer
                .ca_cert_pem,
            pem
        );
        assert!(
            state
                .retry_queue
                .iter()
                .all(|event| !event.last_error.contains("secret-marker"))
        );
        state.pending_endpoint_refresh = None;
        assert!(pending_endpoint_refresh(&state).is_none(), "safe summaries are not pending JSON");
    }

    #[test]
    fn test_legacy_pending_retry_json_remains_readable() {
        let legacy = PendingEndpointRefresh {
            id: "legacy-refresh".to_string(),
            peer: PeerInfo {
                deployment_id: "remote".to_string(),
                ..peer("remote", "https://remote.example.com")
            },
            ..Default::default()
        };
        let state = SiteReplicationState {
            retry_queue: vec![SiteReplicationRetryEvent {
                path: SITE_REPLICATION_ENDPOINT_REFRESH_RETRY_PATH.to_string(),
                last_error: serde_json::to_string(&legacy).expect("serialize legacy pending"),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert_eq!(
            pending_endpoint_refresh(&state).map(|pending| pending.id).as_deref(),
            Some("legacy-refresh")
        );
    }

    #[test]
    fn test_pending_endpoint_refresh_ack_merge_is_monotonic() {
        let latest = PendingEndpointRefresh {
            id: "refresh-acks".to_string(),
            peer: PeerInfo {
                deployment_id: "remote".to_string(),
                ..peer("remote", "https://remote.example.com")
            },
            acked_deployment_ids: BTreeSet::from(["peer-a".to_string()]),
            ..Default::default()
        };
        let stale = PendingEndpointRefresh {
            acked_deployment_ids: BTreeSet::new(),
            ..latest.clone()
        };
        let state = SiteReplicationState {
            pending_endpoint_refresh: Some(latest),
            ..Default::default()
        };

        let merged = merge_pending_endpoint_refresh(&state, &stale, ["peer-b".to_string()]).expect("merge ACKs");
        assert_eq!(merged.acked_deployment_ids, BTreeSet::from(["peer-a".to_string(), "peer-b".to_string()]));
    }

    #[test]
    fn test_internal_endpoint_refresh_retry_is_strictly_idempotent() {
        let committed = PeerInfo {
            deployment_id: "remote".to_string(),
            skip_tls_verify: true,
            ..peer("remote", "https://remote.example.com")
        };
        let state = SiteReplicationState {
            peers: BTreeMap::from([("remote".to_string(), committed.clone())]),
            ..Default::default()
        };

        assert!(internal_endpoint_refresh_already_committed(&state, &committed));
        assert!(!internal_endpoint_refresh_already_committed(
            &state,
            &PeerInfo {
                deployment_id: "other".to_string(),
                ..committed.clone()
            }
        ));
        assert!(!internal_endpoint_refresh_already_committed(
            &state,
            &PeerInfo {
                skip_tls_verify: false,
                ..committed
            }
        ));
    }

    #[tokio::test]
    async fn test_wrong_proposed_ca_fails_before_committed_state_changes() {
        temp_env::async_with_vars([(ALLOW_LOOPBACK_REPLICATION_TARGET_ENV, Some("true"))], async {
            let server_identity = test_tls_identity();
            let wrong_identity = test_tls_identity();
            let (endpoint, server) = spawn_recording_tls_server(
                &server_identity,
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 16\r\nconnection: close\r\n\r\n{\"success\":true}",
            )
            .await;
            let committed = PeerInfo {
                deployment_id: "remote".to_string(),
                ..peer("remote", &endpoint)
            };
            let proposed = PeerInfo {
                ca_cert_pem: wrong_identity.cert_pem,
                ..committed.clone()
            };
            let state = SiteReplicationState {
                peers: BTreeMap::from([("remote".to_string(), committed.clone())]),
                ..Default::default()
            };

            assert!(probe_proposed_peer_tls_transport(&proposed, "access", "secret").await.is_err());
            assert_eq!(state.peers["remote"].ca_cert_pem, committed.ca_cert_pem);
            assert!(state.pending_endpoint_refresh.is_none());
            assert!(server.await.expect("wrong-CA server task").is_none());
        })
        .await;
    }

    #[test]
    fn test_site_replication_bucket_target_replaces_tls_and_preserves_operational_fields() {
        let local = PeerInfo {
            deployment_id: "local".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let remote = PeerInfo {
            deployment_id: "remote".to_string(),
            skip_tls_verify: true,
            ..peer("remote", "https://remote.example.com:9443")
        };
        let state = SiteReplicationState {
            service_account_access_key: "svc".to_string(),
            peers: BTreeMap::from([("local".to_string(), local.clone()), ("remote".to_string(), remote.clone())]),
            ..Default::default()
        };
        let generated = site_replication_bucket_target_for_peer("photos", &state, &remote, "secret", None)
            .expect("build target")
            .expect("target exists");
        assert!(generated.skip_tls_verify);
        assert_eq!(generated.ca_cert_pem, "");

        let existing = BucketTarget {
            arn: generated.arn,
            endpoint: "remote.example.com:9443".to_string(),
            secure: true,
            target_type: BucketTargetType::ReplicationService,
            deployment_id: "remote".to_string(),
            skip_tls_verify: false,
            ca_cert_pem: "old-ca".to_string(),
            bandwidth_limit: 42,
            disable_proxy: true,
            ..Default::default()
        };
        let reconciled = reconcile_site_replication_bucket_targets(
            BucketTargets { targets: vec![existing] },
            "photos",
            &state,
            &local,
            None,
            "secret",
        )
        .expect("reconcile targets");
        let target = reconciled.targets.first().expect("reconciled target");
        assert!(target.skip_tls_verify);
        assert_eq!(target.ca_cert_pem, "");
        assert_eq!(target.bandwidth_limit, 42);
        assert!(target.disable_proxy);
    }

    #[test]
    fn test_peer_tls_capability_query_is_supported_and_legacy_response_fails_closed() {
        let remote = peer("remote", "https://remote.example.com");

        assert!(peer_edit_capability_supported("peer-tls-settings"));
        assert!(peer_edit_capability_supported("endpoint-target-refresh"));
        assert!(!peer_edit_capability_supported("unknown"));
        assert!(peer_capability_response_supported(&remote, StatusCode::OK, br#"{"success":true}"#).expect("supported"));
        assert!(!peer_capability_response_supported(&remote, StatusCode::NOT_FOUND, b"").expect("legacy peer"));
    }

    /// P1-15 PR2: the add's finalize fan-out delivers peer edits after its
    /// state transaction has been released — nothing may hold the state-object
    /// lock across peer traffic. Ordering therefore rests entirely on the
    /// generation allocated in that commit: an unstamped delivery is applied
    /// by the receiver in arrival order, which is what the removed process
    /// guard used to paper over (and never could across two nodes).
    #[test]
    fn add_handler_fans_out_peer_edits_under_the_committed_generation() {
        let src = include_str!("site_replication.rs");
        let add = src
            .split("impl Operation for SiteReplicationAddHandler")
            .nth(1)
            .and_then(|rest| rest.split("pub struct SiteReplicationRemoveHandler").next())
            .expect("add handler block");

        assert!(
            add.contains("next_peer_edit_generation"),
            "the add must allocate a fan-out generation (inside the committing transaction)"
        );
        assert!(
            add.contains("peer_edit_path_with_fence"),
            "the add's finalize fan-out must carry the committed generation fence"
        );
        assert!(
            !add.contains("SITE_REPLICATION_PEER_EDIT_PATH"),
            "the finalize fan-out must not fall back to the unstamped peer-edit path"
        );
    }

    #[test]
    fn test_tls_capability_gates_run_before_add_or_edit_state_side_effects() {
        let src = include_str!("site_replication.rs");
        let add = src
            .split("impl Operation for SiteReplicationAddHandler")
            .nth(1)
            .and_then(|rest| rest.split("pub struct SiteReplicationRemoveHandler").next())
            .expect("add handler block");
        let edit = src
            .split("impl Operation for SiteReplicationEditHandler")
            .nth(1)
            .and_then(|rest| rest.split("pub struct SRPeerEditCapabilitiesHandler").next())
            .expect("edit handler block");

        assert!(
            add.find("require_add_peer_tls_capability").expect("add capability gate")
                < add
                    .find("ensure_site_replicator_service_account")
                    .expect("service-account creation"),
            "add capability gate must run before service-account creation"
        );
        assert!(
            edit.find("require_edit_peer_tls_capability").expect("edit capability gate")
                < edit.find("set_pending_endpoint_refresh").expect("pending state write"),
            "edit capability gate must run before pending state is recorded"
        );
        assert!(
            edit.find("require_edit_peer_tls_capability").expect("edit capability gate")
                < edit.find("update_site_replication_state(").expect("state commit"),
            "edit capability gate must run before the state is committed"
        );
    }

    #[derive(Clone, Default)]
    struct LegacyPeerTestState {
        requests: Arc<StdMutex<Vec<String>>>,
        minio_operation_supported: Arc<AtomicBool>,
    }

    async fn legacy_peer_test_handler(
        State(state): State<LegacyPeerTestState>,
        method: Method,
        uri: Uri,
    ) -> (StatusCode, String) {
        state
            .requests
            .lock()
            .expect("legacy peer request log")
            .push(format!("{method} {}", uri.path_and_query().map_or("/", |value| value.as_str())));
        match (method, uri.path()) {
            (Method::GET, "/minio/admin/v3/site-replication/metainfo") => {
                (StatusCode::OK, r#"{"Buckets":{"photos":{}}}"#.to_string())
            }
            (Method::PUT, "/minio/admin/v3/site-replication/peer/edit") => (StatusCode::OK, String::new()),
            (Method::PUT, "/minio/admin/v3/site-replication/peer/bucket-ops")
                if !uri
                    .query()
                    .is_some_and(|query| query.contains("operation=ConfigureReplication"))
                    || state.minio_operation_supported.load(Ordering::Relaxed) =>
            {
                (StatusCode::OK, String::new())
            }
            _ => (StatusCode::NOT_FOUND, String::new()),
        }
    }

    #[tokio::test]
    #[serial]
    async fn legacy_endpoint_refresh_executes_peer_edit_and_bucket_repair() {
        temp_env::async_with_vars(
            [(ALLOW_LOOPBACK_REPLICATION_TARGET_ENV, Some("true"))],
            legacy_endpoint_refresh_executes_peer_edit_and_bucket_repair_inner(),
        )
        .await;
    }

    async fn legacy_endpoint_refresh_executes_peer_edit_and_bucket_repair_inner() {
        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(err) => panic!("bind legacy peer test server: {err}"),
        };
        let endpoint = format!("http://{}", listener.local_addr().expect("legacy peer test address"));
        let state = LegacyPeerTestState::default();
        state.minio_operation_supported.store(true, Ordering::Relaxed);
        let requests = state.requests.clone();
        let minio_operation_supported = state.minio_operation_supported.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, Router::new().fallback(any(legacy_peer_test_handler)).with_state(state))
                .await
                .expect("serve legacy peer test requests");
        });

        let target = PeerInfo {
            deployment_id: "remote".to_string(),
            endpoint: endpoint.clone(),
            ..Default::default()
        };
        let pending = PendingEndpointRefresh {
            id: "refresh-legacy".to_string(),
            peer: PeerInfo {
                deployment_id: "remote".to_string(),
                endpoint,
                ..Default::default()
            },
            ..Default::default()
        };

        refresh_legacy_peer_bucket_targets(&target, &pending, "site-replicator-0", "test-secret")
            .await
            .expect("legacy peer endpoint refresh");
        assert_eq!(
            *requests.lock().expect("legacy peer request log"),
            vec![
                "PUT /minio/admin/v3/site-replication/peer/edit".to_string(),
                "GET /minio/admin/v3/site-replication/metainfo?buckets=true".to_string(),
                "PUT /minio/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=ConfigureReplication".to_string(),
            ]
        );

        requests.lock().expect("legacy peer request log").clear();
        minio_operation_supported.store(false, Ordering::Relaxed);
        refresh_legacy_peer_bucket_targets(&target, &pending, "site-replicator-0", "test-secret")
            .await
            .expect("legacy RustFS peer endpoint refresh");
        server.abort();
        assert_eq!(
            *requests.lock().expect("legacy peer request log"),
            vec![
                "PUT /minio/admin/v3/site-replication/peer/edit".to_string(),
                "GET /minio/admin/v3/site-replication/metainfo?buckets=true".to_string(),
                "PUT /minio/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=ConfigureReplication".to_string(),
                "PUT /minio/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=configure-replication".to_string(),
            ]
        );
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

    // The replication service account must be able to carry object-lock metadata to the peer.
    // Without these actions the peer answers AccessDenied for any replicated object that has
    // retention or a legal hold, so a WORM-protected object never reaches the replica at all,
    // and a retention change made after upload never propagates.
    #[tokio::test]
    async fn test_site_replicator_policy_allows_object_lock_replication() {
        let policy = site_replicator_service_account_policy().expect("site replicator policy should parse");
        let groups: Option<Vec<String>> = None;
        let claims = HashMap::new();
        let conditions = HashMap::new();

        let base_args = rustfs_policy::policy::Args {
            account: SITE_REPLICATOR_SERVICE_ACCOUNT,
            groups: &groups,
            action: Action::S3Action(S3Action::PutObjectRetentionAction),
            conditions: &conditions,
            is_owner: false,
            claims: &claims,
            deny_only: false,
            bucket: "photos",
            object: "image.jpg",
        };

        for action in [
            S3Action::PutObjectRetentionAction,
            S3Action::GetObjectRetentionAction,
            S3Action::PutObjectLegalHoldAction,
            S3Action::GetObjectLegalHoldAction,
        ] {
            let args = rustfs_policy::policy::Args {
                action: Action::S3Action(action),
                ..base_args
            };
            assert!(
                policy.is_allowed(&args).await,
                "site replicator must be allowed to replicate object-lock metadata: {action:?}"
            );
        }

        // Governance bypass stays denied: replication must not be able to erase a retained
        // version on the peer.
        let bypass_args = rustfs_policy::policy::Args {
            action: Action::S3Action(S3Action::BypassGovernanceRetentionAction),
            ..base_args
        };
        assert!(
            !policy.is_allowed(&bypass_args).await,
            "site replicator must not be granted governance bypass"
        );
    }

    #[test]
    fn test_sr_peer_edit_handler_uses_site_replication_operation_action() {
        let src = include_str!("site_replication.rs");
        let handler_block = src
            .split("impl Operation for SRPeerEditHandler")
            .nth(1)
            .and_then(|rest| rest.split("pub struct SRPeerRemoveHandler").next())
            .expect("SRPeerEditHandler block should exist");

        assert!(
            handler_block
                .contains("validate_site_replication_admin_request(&req, AdminAction::SiteReplicationOperationAction).await?;"),
            "SRPeerEditHandler should authorize internal peer edits with SiteReplicationOperationAction"
        );
        assert!(
            !handler_block
                .contains("validate_site_replication_admin_request(&req, AdminAction::SiteReplicationAddAction).await?;"),
            "SRPeerEditHandler must not require SiteReplicationAddAction for internal peer edits"
        );
        // P1-15 review follow-up: the ordering fence is only worth anything if
        // the handler both rejects a superseded delivery and raises the mark it
        // rejects against — dropping either half silently restores
        // last-writer-wins between two nodes of the sending site.
        assert!(
            handler_block.contains("peer_edit_delivery_is_stale(state, origin, *generation)"),
            "SRPeerEditHandler must reject peer edits a newer generation already superseded"
        );
        assert!(
            handler_block.contains("record_applied_peer_edit_generation(state, origin, *generation);"),
            "SRPeerEditHandler must record the applied generation so later stale deliveries are recognised"
        );
        // P1-15 PR2: both halves of the fence and the edit they fence share
        // ONE transaction. Checking the fence against a state read outside the
        // lock would let the check pass on one snapshot and the write land on
        // another — which is the interleaving the fence exists to reject.
        assert!(
            handler_block.contains("update_site_replication_state_when_changed(move |state| {"),
            "SRPeerEditHandler must take the fence decision inside the state transaction"
        );
        assert!(
            !handler_block.contains("save_site_replication_state("),
            "SRPeerEditHandler must not write the state outside the transaction boundary"
        );

        let sender_block = src
            .split("impl Operation for SiteReplicationEditHandler")
            .nth(1)
            .and_then(|rest| rest.split("pub struct SRPeerEditCapabilitiesHandler").next())
            .expect("SiteReplicationEditHandler block should exist");
        assert!(
            sender_block.contains("Ok((next_peer_edit_generation(state), peers_to_send))"),
            "the edit handler must allocate the generation inside the committed state, not outside the lock"
        );
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

        assert!(site_replication_rule_complete(&site_a_config.rules[0], "site-a"));
        assert_eq!(
            site_replication_config_mismatch(vec![("site-a", Some(&site_a_xml)), ("site-b", Some(&site_b_xml))].into_iter(), 2),
            (2, false)
        );
    }

    // A site whose rules are well-formed but whose peer endpoint it cannot reach builds no
    // target client, so it replicates nothing while its rule set still reads as correct.
    // Rule-shape checking alone cannot see that, so the reporting site says so directly.
    #[test]
    fn test_merge_bucket_status_reports_offline_targets_as_mismatch() {
        // Both sites carry a correct, peer-specific rule set: rule-shape checking alone
        // sees a healthy pair. Only the reported target health distinguishes them.
        let site_info = |peer: &str, targets_online: Option<bool>| {
            let xml = String::from_utf8(serialize(&site_repl_config(peer)).unwrap()).unwrap();
            let mut info = SRInfo::default();
            info.buckets.insert(
                "photos".to_string(),
                SRBucketInfo {
                    bucket: "photos".to_string(),
                    replication_config: Some(xml),
                    replication_targets_online: targets_online,
                    ..Default::default()
                },
            );
            info
        };

        let mut status = SRStatusInfo::default();
        let site_infos = BTreeMap::from([
            ("site-a".to_string(), site_info("site-b", Some(true))),
            ("site-b".to_string(), site_info("site-a", Some(false))),
        ]);
        merge_bucket_status_info(&mut status, &site_infos, &SRStatusOptions::default());

        let summary = status
            .bucket_stats
            .get("photos")
            .and_then(|per_site| per_site.get("site-a"))
            .expect("bucket stats should carry a per-site summary");
        assert!(
            summary.replication_cfg_mismatch,
            "a peer reporting an offline replication target must not read as in sync"
        );
    }

    // A peer that predates the field reports nothing; that is unknown, not a fault, and must
    // not flip every bucket to out-of-sync during a mixed-version upgrade.
    #[test]
    fn test_merge_bucket_status_treats_absent_target_health_as_unknown() {
        let site_info = |peer: &str| {
            let xml = String::from_utf8(serialize(&site_repl_config(peer)).unwrap()).unwrap();
            let mut info = SRInfo::default();
            info.buckets.insert(
                "photos".to_string(),
                SRBucketInfo {
                    bucket: "photos".to_string(),
                    replication_config: Some(xml),
                    replication_targets_online: None,
                    ..Default::default()
                },
            );
            info
        };

        let mut status = SRStatusInfo::default();
        let site_infos = BTreeMap::from([
            ("site-a".to_string(), site_info("site-b")),
            ("site-b".to_string(), site_info("site-a")),
        ]);
        merge_bucket_status_info(&mut status, &site_infos, &SRStatusOptions::default());

        let summary = status
            .bucket_stats
            .get("photos")
            .and_then(|per_site| per_site.get("site-a"))
            .expect("bucket stats should carry a per-site summary");
        assert!(
            !summary.replication_cfg_mismatch,
            "peers that do not report target health must not be treated as broken"
        );
    }

    // The one-directional regression: a `replication-config` broadcast overwrote the receiver's
    // rules with the sender's, leaving both sites holding byte-identical XML whose destination
    // ARN names the receiver. Only one site could push, yet the status check counted rules and
    // reported "in sync" — the operator's single health signal agreed with the broken state.
    #[test]
    fn test_site_replication_config_mismatch_rejects_rule_pointing_at_owning_site() {
        let shared_config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![build_site_replication_rule(
                "arn:rustfs:replication::site-b:test-replication",
                1,
                "site-repl-site-b",
            )],
        };
        let shared_xml = String::from_utf8(serialize(&shared_config).expect("site replication XML should serialize"))
            .expect("site replication XML should be UTF-8");

        assert!(
            !site_replication_rule_complete(&shared_config.rules[0], "site-b"),
            "a rule whose destination ARN names its own site can never replicate"
        );
        assert_eq!(
            site_replication_config_mismatch(vec![("site-a", Some(&shared_xml)), ("site-b", Some(&shared_xml))].into_iter(), 2),
            (2, true),
            "identical configs mean site-b points at itself and cannot push"
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

    #[tokio::test]
    #[serial]
    async fn test_add_bootstrap_scope_only_allows_expected_bucket_setup_until_guard_drops() {
        let token;
        {
            let lifecycle = SiteReplicationLifecycleGuard::acquire().await;
            let guard = SiteReplicationAddInProgressGuard::start(lifecycle, HashSet::from(["legacy-bucket".to_string()]))
                .expect("start site replication add guard");
            token = guard.token.to_string();
            assert!(bootstrap_peer_bucket_operation_allowed(
                "new-bucket",
                "make-with-versioning",
                Some(&token)
            ));
            assert!(bootstrap_peer_bucket_operation_allowed(
                "new-bucket",
                "configure-replication",
                Some(&token)
            ));
            assert!(bootstrap_peer_bucket_operation_allowed("legacy-bucket", "make-with-versioning", None));
            assert!(!bootstrap_peer_bucket_operation_allowed(
                "unexpected-bucket",
                "make-with-versioning",
                None
            ));
            assert!(!bootstrap_peer_bucket_operation_allowed(
                "legacy-bucket",
                "force-delete-bucket",
                Some(&token)
            ));
            assert!(!bootstrap_peer_bucket_operation_allowed(
                "legacy-bucket",
                "make-with-versioning",
                Some(&Uuid::new_v4().to_string())
            ));
        }
        assert!(!bootstrap_peer_bucket_operation_allowed(
            "new-bucket",
            "make-with-versioning",
            Some(&token)
        ));
    }

    #[test]
    fn test_add_bootstrap_token_round_trips_from_join_to_bucket_operation() {
        let token = Uuid::new_v4().to_string();
        let join_path = with_site_replication_bootstrap_token(SITE_REPLICATION_PEER_JOIN_PATH, &token);
        let join_uri: Uri = join_path.parse().expect("parse peer join path");
        let received_token = site_replication_bootstrap_token(&join_uri).expect("peer join bootstrap token");
        let bucket_path =
            with_site_replication_bootstrap_token(&bootstrap_bucket_op_path("photos", "configure-replication"), &received_token);
        let bucket_uri: Uri = bucket_path.parse().expect("parse bucket operation path");
        let query = query_pairs(&bucket_uri);

        assert_eq!(query.get("bootstrapToken"), Some(&token));
        assert_eq!(query.get("operation").map(String::as_str), Some("configure-replication"));
    }

    #[tokio::test]
    #[serial]
    async fn test_add_lifecycle_allows_callback_before_remove_writer() {
        let lifecycle = SiteReplicationLifecycleGuard::acquire().await;
        let add_guard =
            SiteReplicationAddInProgressGuard::start(lifecycle, HashSet::new()).expect("start site replication add guard");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (entered_tx, mut entered_rx) = tokio::sync::oneshot::channel();
        let remove = tokio::spawn(async move {
            let _ = started_tx.send(());
            let _lifecycle = SiteReplicationLifecycleGuard::acquire().await;
            let _bucket_op = SITE_REPLICATION_BUCKET_OP_LOCK.write().await;
            let _ = entered_tx.send(());
        });
        started_rx.await.expect("remove task started");

        let callback = tokio::time::timeout(Duration::from_millis(500), SITE_REPLICATION_BUCKET_OP_LOCK.read())
            .await
            .expect("callback read lock should not wait behind remove");
        assert!(matches!(entered_rx.try_recv(), Err(tokio::sync::oneshot::error::TryRecvError::Empty)));

        drop(callback);
        drop(add_guard);
        tokio::time::timeout(Duration::from_millis(500), remove)
            .await
            .expect("remove should enter after add finishes")
            .expect("remove task should finish");
        entered_rx.await.expect("remove entered lifecycle");
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
                ..PeerSite::default()
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
                    ..PeerSite::default()
                },
                PeerSite {
                    name: "remote".to_string(),
                    endpoint: "https://remote.example.com".to_string(),
                    access_key: "remote-ak".to_string(),
                    secret_key: "remote-sk".to_string(),
                    ..PeerSite::default()
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

    // Console fix: the web UI omits the local deployment from the add payload. ensure_local_site_present
    // injects it so the add preflight (which requires the local deployment) succeeds.
    #[test]
    fn test_ensure_local_site_present_injects_when_missing() {
        let local_peer = peer("local", "https://local.example.com");
        let mut sites = vec![PeerSite {
            name: "remote".to_string(),
            endpoint: "https://remote.example.com".to_string(),
            access_key: "remote-ak".to_string(),
            secret_key: "remote-sk".to_string(),
            ..Default::default()
        }];

        ensure_local_site_present(&mut sites, &local_peer);

        assert_eq!(sites.len(), 2, "the local site must be injected when missing");
        assert!(
            sites
                .iter()
                .any(|s| same_identity_endpoint(&s.endpoint, &local_peer.endpoint)),
            "an injected site must match the local endpoint"
        );
        // The console payload (remote-only) now validates end-to-end at the add-sites stage.
        validate_add_sites(&sites, &local_peer).expect("add sites must validate after injecting the local site");
    }

    #[test]
    fn test_ensure_local_site_present_noop_when_already_included() {
        let local_peer = peer("local", "https://local.example.com");
        let mut sites = vec![
            PeerSite {
                name: "local".to_string(),
                endpoint: "https://local.example.com".to_string(),
                ..Default::default()
            },
            PeerSite {
                name: "remote".to_string(),
                endpoint: "https://remote.example.com".to_string(),
                access_key: "remote-ak".to_string(),
                secret_key: "remote-sk".to_string(),
                ..Default::default()
            },
        ];

        ensure_local_site_present(&mut sites, &local_peer);

        assert_eq!(sites.len(), 2, "the local site must not be duplicated when already present");
        assert_eq!(
            sites
                .iter()
                .filter(|s| same_identity_endpoint(&s.endpoint, &local_peer.endpoint))
                .count(),
            1,
            "exactly one local site entry"
        );
    }

    fn preflight_site(name: &str, endpoint: &str, deployment_id: &str, bucket_count: usize) -> SiteReplicationAddPreflightInfo {
        SiteReplicationAddPreflightInfo {
            name: name.to_string(),
            endpoint: endpoint.to_string(),
            deployment_id: deployment_id.to_string(),
            enabled: false,
            bucket_count,
            bucket_names: HashSet::new(),
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
                user_type: sr_wire_user_type(UserType::Reg, false),
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
    fn test_site_replication_repair_request_is_strict_and_requires_explicit_mode() {
        assert!(serde_json::from_str::<SiteReplicationRepairRequest>(r#"{"mode":"dry-run"}"#).is_ok());
        assert!(serde_json::from_str::<SiteReplicationRepairRequest>(r#"{"mode":"execute"}"#).is_ok());
        assert!(serde_json::from_str::<SiteReplicationRepairRequest>(r#"{}"#).is_err());
        assert!(serde_json::from_str::<SiteReplicationRepairRequest>(r#"{"mode":"dry-run","secret":"leak"}"#).is_err());
    }

    #[test]
    fn test_site_replication_repair_dry_run_plan_is_non_mutating_and_redacted() {
        let state = SiteReplicationState {
            name: "local".to_string(),
            service_account_access_key: "site-replicator-0".to_string(),
            service_account_secret_key: "state-secret".to_string(),
            peers: BTreeMap::from([
                (
                    "local-dep".to_string(),
                    PeerInfo {
                        deployment_id: "local-dep".to_string(),
                        ..peer("local", "https://local.example.com")
                    },
                ),
                (
                    "remote-dep".to_string(),
                    PeerInfo {
                        deployment_id: "remote-dep".to_string(),
                        ..peer("remote", "https://remote.example.com")
                    },
                ),
            ]),
            retry_queue: vec![SiteReplicationRetryEvent {
                peer_deployment_id: "remote-dep".to_string(),
                path: format!(
                    "{SITE_REPLICATION_PEER_BUCKET_OPS_PATH}?bucket=photos&operation={SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING}"
                ),
                last_error: "credential=retry-secret".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let plan = SiteReplicationBootstrapPlan {
            iam_items: vec![SRIAMItem {
                r#type: "iam-user".to_string(),
                iam_user: Some(rustfs_madmin::SRIAMUser {
                    access_key: "alice".to_string(),
                    user_req: Some(AddOrUpdateUserReq {
                        secret_key: "iam-secret".to_string(),
                        policy: None,
                        status: rustfs_madmin::AccountStatus::Enabled,
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            bucket_make_ops: vec![format!(
                "{SITE_REPLICATION_PEER_BUCKET_OPS_PATH}?bucket=photos&operation={SITE_REPLICATION_BUCKET_OP_MAKE_WITH_VERSIONING}"
            )],
            ..Default::default()
        };
        let before = serde_json::to_vec(&state).expect("serialize state before planning");
        let local = state.peers.get("local-dep").expect("local peer");

        let response = SiteReplicationRepairPreflight {
            mode: "dry-run",
            status: "planned",
            preflight_token: site_replication_repair_preflight_token(&state, &plan, b"test-signing-key")
                .expect("preflight token"),
            retry_events: state.retry_queue.len(),
            sites: site_replication_repair_sites(&state, local, &plan, b"test-signing-key").expect("repair sites"),
        };
        let encoded = serde_json::to_string(&response).expect("serialize preflight");

        assert_eq!(serde_json::to_vec(&state).expect("serialize state after planning"), before);
        assert!(!encoded.contains("state-secret"));
        assert!(!encoded.contains("iam-secret"));
        assert!(!encoded.contains("retry-secret"));
        assert!(!encoded.contains("remote.example.com"));
        assert_eq!(response.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_IAM_FAMILY].planned, 1);
        let bucket_family = &response.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY];
        assert_eq!(bucket_family.retry_events, 1);
        let task_id = &bucket_family.tasks[0].task_id;
        assert_eq!(task_id.len(), 43);
        assert!(
            task_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        );
        assert!(!task_id.contains("bucket"));
        assert!(!task_id.contains("photos"));
        assert!(!task_id.contains("remote-dep"));
        assert_eq!(bucket_family.tasks[0].status, "planned");
        let repeated = site_replication_repair_sites(&state, local, &plan, b"test-signing-key").expect("repeat repair sites");
        assert_eq!(
            task_id,
            &repeated["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY].tasks[0].task_id
        );
        let rotated = site_replication_repair_sites(&state, local, &plan, b"rotated-signing-key").expect("rotated repair sites");
        assert_ne!(
            task_id,
            &rotated["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY].tasks[0].task_id
        );
    }

    #[test]
    fn test_site_replication_repair_preflight_detects_stale_snapshot() {
        let mut state = SiteReplicationState {
            name: "local".to_string(),
            service_account_access_key: "site-replicator-0".to_string(),
            peers: BTreeMap::from([(
                "remote-dep".to_string(),
                PeerInfo {
                    deployment_id: "remote-dep".to_string(),
                    ..peer("remote", "https://remote.example.com")
                },
            )]),
            ..Default::default()
        };
        let plan = SiteReplicationBootstrapPlan {
            bucket_make_ops: vec![
                "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning".to_string(),
            ],
            ..Default::default()
        };
        let original = site_replication_repair_preflight_token(&state, &plan, b"test-signing-key").expect("original token");
        let original_plan = site_replication_repair_plan_token(&state, &plan).expect("original plan token");

        state.updated_at = Some(OffsetDateTime::UNIX_EPOCH);
        let changed = site_replication_repair_preflight_token(&state, &plan, b"test-signing-key").expect("changed token");
        let changed_plan = site_replication_repair_plan_token(&state, &plan).expect("changed plan token");

        assert_ne!(original, changed);
        assert_eq!(original.len(), 43);
        assert!(
            original
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        );
        assert_ne!(
            changed,
            site_replication_repair_preflight_token(&state, &plan, b"different-signing-key").expect("differently signed token")
        );
        assert!(site_replication_repair_preflight_token(&state, &plan, b"").is_err());

        state.retry_queue.push(SiteReplicationRetryEvent {
            id: "retry-1".to_string(),
            peer_deployment_id: "remote-dep".to_string(),
            path: "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning".to_string(),
            ..Default::default()
        });
        let retry_changed =
            site_replication_repair_preflight_token(&state, &plan, b"test-signing-key").expect("retry-aware token");
        assert_ne!(changed, retry_changed);
        assert_eq!(
            changed_plan,
            site_replication_repair_plan_token(&state, &plan).expect("retry-stable plan token")
        );
        assert_ne!(original_plan, changed_plan, "updated_at changes the plan token");
    }

    #[test]
    fn test_site_replication_repair_partial_retry_skips_completed_tasks_and_survives_restart() {
        let local = PeerInfo {
            deployment_id: "local-dep".to_string(),
            ..peer("local", "https://local.example.com")
        };
        let remote = PeerInfo {
            deployment_id: "remote-dep".to_string(),
            ..peer("remote", "https://remote.example.com")
        };
        let state = SiteReplicationState {
            peers: BTreeMap::from([
                (local.deployment_id.clone(), local.clone()),
                (remote.deployment_id.clone(), remote.clone()),
            ]),
            ..Default::default()
        };
        let plan = SiteReplicationBootstrapPlan {
            iam_items: vec![SRIAMItem {
                r#type: "policy".to_string(),
                name: "readwrite".to_string(),
                ..Default::default()
            }],
            bucket_make_ops: vec![
                "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning".to_string(),
            ],
            ..Default::default()
        };
        let tasks = site_replication_repair_tasks(&plan);
        let (first_index, first_task) = &tasks[0];
        let (second_index, second_task) = &tasks[1];
        let now = OffsetDateTime::UNIX_EPOCH;
        let mut operation = SiteReplicationRepairOperation {
            operation_id: Uuid::new_v4().to_string(),
            preflight_token: site_replication_repair_preflight_token(&state, &plan, b"test-signing-key")
                .expect("preflight token"),
            plan_token: site_replication_repair_plan_token(&state, &plan).expect("plan token"),
            status: "running".to_string(),
            sites: site_replication_repair_sites(&state, &local, &plan, b"test-signing-key").expect("repair sites"),
            created_at: Some(now),
            updated_at: Some(now),
            completed_at: None,
        };

        update_site_replication_repair_task(&mut operation, &remote.deployment_id, first_task.family(), *first_index, Ok(()))
            .expect("record first success");
        update_site_replication_repair_task(
            &mut operation,
            &remote.deployment_id,
            second_task.family(),
            *second_index,
            Err("peer response included secret=must-not-leak"),
        )
        .expect("record injected failure");
        summarize_site_replication_repair_operation(&mut operation);
        assert_eq!(operation.status, "partial");
        assert_eq!(
            operation.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_IAM_FAMILY].tasks[0].status,
            "succeeded"
        );
        assert_eq!(
            operation.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY].tasks[0].status,
            "failed"
        );
        assert!(
            !site_replication_repair_task_pending(&operation, &remote.deployment_id, first_task.family(), *first_index)
                .expect("first task state")
        );
        assert!(
            !site_replication_repair_task_pending(&operation, &remote.deployment_id, second_task.family(), *second_index)
                .expect("failed task waits for retry")
        );
        let response = serde_json::to_string(&site_replication_repair_operation_response(&operation))
            .expect("serialize public operation response");
        assert!(!response.contains(&operation.preflight_token));
        assert!(!response.contains(&operation.plan_token));

        let persisted_state = SiteReplicationRepairState {
            operations: BTreeMap::from([(operation.operation_id.clone(), operation)]),
        };
        let encoded = serde_json::to_vec(&persisted_state).expect("persist state");
        let recovered_state: SiteReplicationRepairState = serde_json::from_slice(&encoded).expect("load state after restart");
        let mut recovered = recovered_state
            .operations
            .into_values()
            .next()
            .expect("recover operation after restart");
        assert_eq!(recovered.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_IAM_FAMILY].succeeded, 1);
        assert!(!String::from_utf8(encoded).expect("operation JSON").contains("must-not-leak"));

        prepare_site_replication_repair_retry(&mut recovered);
        assert_eq!(
            recovered.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_IAM_FAMILY].tasks[0].status,
            "skipped"
        );
        assert_eq!(
            recovered.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY].tasks[0].status,
            "planned"
        );
        assert!(
            site_replication_repair_task_pending(&recovered, &remote.deployment_id, second_task.family(), *second_index)
                .expect("failed task becomes retryable")
        );
        update_site_replication_repair_task(&mut recovered, &remote.deployment_id, second_task.family(), *second_index, Ok(()))
            .expect("retry failed task");
        assert!(
            !site_replication_repair_task_pending(&recovered, &remote.deployment_id, first_task.family(), *first_index)
                .expect("completed task remains skipped")
        );
        summarize_site_replication_repair_operation(&mut recovered);

        assert_eq!(recovered.status, "success");
        assert_eq!(recovered.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_IAM_FAMILY].succeeded, 1);
        assert_eq!(recovered.sites["remote-dep"].families[SITE_REPLICATION_REPAIR_BUCKET_FAMILY].succeeded, 1);
    }

    #[test]
    fn test_site_replication_repair_error_classification_is_redacted() {
        assert_eq!(
            classify_site_replication_repair_error(
                "peer request to https://user:secret@example.com failed with 403: token=private"
            ),
            "authorization-failed"
        );
        assert_eq!(
            classify_site_replication_repair_error("peer request body contained secret=private"),
            "remote-operation-failed"
        );
    }

    #[test]
    fn test_site_replication_repair_admission_resumes_same_id_and_rejects_conflicts() {
        let existing = SiteReplicationRepairOperation {
            operation_id: "operation-a".to_string(),
            preflight_token: "preflight-a".to_string(),
            plan_token: "plan-a".to_string(),
            status: "running".to_string(),
            ..Default::default()
        };
        let mut state = SiteReplicationRepairState {
            operations: BTreeMap::from([(existing.operation_id.clone(), existing.clone())]),
        };

        let resumed = admit_site_replication_repair_operation(
            &mut state,
            existing.operation_id.clone(),
            &existing.preflight_token,
            existing.clone(),
        )
        .expect("same operation ID and preflight should resume");
        assert_eq!(resumed.operation_id, existing.operation_id);

        let conflicting_operation = SiteReplicationRepairOperation {
            operation_id: "operation-b".to_string(),
            preflight_token: "preflight-b".to_string(),
            plan_token: "plan-b".to_string(),
            status: "running".to_string(),
            ..Default::default()
        };
        let conflicting_preflight = conflicting_operation.preflight_token.clone();
        let err = admit_site_replication_repair_operation(
            &mut state,
            conflicting_operation.operation_id.clone(),
            &conflicting_preflight,
            conflicting_operation,
        )
        .expect_err("a different operation must not pass a persisted running operation");
        assert_eq!(err.code(), &S3ErrorCode::ClientTokenConflict);

        let stale_candidate = SiteReplicationRepairOperation {
            plan_token: "plan-changed".to_string(),
            ..existing.clone()
        };
        let err = admit_site_replication_repair_operation(
            &mut state,
            existing.operation_id.clone(),
            &existing.preflight_token,
            stale_candidate,
        )
        .expect_err("a resumed operation must remain bound to its original plan");
        assert_eq!(err.code(), &S3ErrorCode::PreconditionFailed);

        let err =
            admit_site_replication_repair_operation(&mut state, existing.operation_id.clone(), "different-preflight", existing)
                .expect_err("an operation ID must remain bound to its original preflight");
        assert_eq!(err.code(), &S3ErrorCode::ClientTokenConflict);
    }

    #[test]
    fn test_site_replication_repair_history_never_prunes_retriable_operations() {
        let mut operations = (0..=SITE_REPLICATION_REPAIR_OPERATION_LIMIT)
            .map(|index| {
                (
                    format!("success-{index}"),
                    SiteReplicationRepairOperation {
                        operation_id: format!("success-{index}"),
                        status: "success".to_string(),
                        created_at: OffsetDateTime::from_unix_timestamp(i64::try_from(index).expect("small test index")).ok(),
                        ..Default::default()
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        operations.insert(
            "partial".to_string(),
            SiteReplicationRepairOperation {
                operation_id: "partial".to_string(),
                status: "partial".to_string(),
                created_at: Some(OffsetDateTime::UNIX_EPOCH),
                ..Default::default()
            },
        );

        prune_site_replication_repair_operations(&mut operations);

        assert!(operations.contains_key("partial"));
        assert_eq!(operations.len(), SITE_REPLICATION_REPAIR_OPERATION_LIMIT);
        assert!(!operations.contains_key("success-0"));
        assert!(!operations.contains_key("success-1"));
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

        upsert_site_replication_retry_event(&mut queue, &peer, "/rustfs/admin/v3/site-replication/peer/iam-item", "first", None);
        upsert_site_replication_retry_event(&mut queue, &peer, "/rustfs/admin/v3/site-replication/peer/iam-item", "second", None);
        upsert_site_replication_retry_event(&mut queue, &peer, "/rustfs/admin/v3/site-replication/peer/iam-item", "third", None);

        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].retry_count, SITE_REPLICATION_RETRY_FAILED_AFTER);
        assert!(queue[0].failed);
        assert_eq!(queue[0].last_error, "third");
    }

    /// P1-15 review follow-up: a successful peer-edit delivery only proves the
    /// peer reached the state THAT delivery carried. Settling it must not
    /// erase a retry event a newer edit left behind, or the local site sits on
    /// edit B, the peer on edit A, and nothing is queued to converge them.
    #[test]
    fn retry_settlement_must_not_erase_a_newer_generation_failure() {
        let peer = PeerInfo {
            deployment_id: "remote-dep".to_string(),
            ..peer("remote", "https://remote.example.com")
        };
        let mut queue = Vec::new();

        // Edit A (generation 5) delivered successfully and is stalled before
        // settling. Edit B (generation 6) commits meanwhile, fails delivery to
        // the same peer, and enqueues.
        upsert_site_replication_retry_event(&mut queue, &peer, SITE_REPLICATION_PEER_EDIT_PATH, "peer offline", Some(6));

        // A resumes: its own settlement must leave B's retry alone.
        assert_eq!(
            settle_site_replication_retry_events(&mut queue, &peer, SITE_REPLICATION_PEER_EDIT_PATH, Some(5)),
            0
        );
        assert_eq!(queue.len(), 1, "the newer edit's retry event was erased by an older success");
        assert_eq!(queue[0].edit_generation, Some(6));

        // An even older delivery failing afterwards must not lower the fence.
        upsert_site_replication_retry_event(&mut queue, &peer, SITE_REPLICATION_PEER_EDIT_PATH, "still offline", Some(4));
        assert_eq!(queue[0].edit_generation, Some(6));

        // B's own delivery succeeding is what clears it.
        assert_eq!(
            settle_site_replication_retry_events(&mut queue, &peer, SITE_REPLICATION_PEER_EDIT_PATH, Some(6)),
            1
        );
        assert!(queue.is_empty());

        // Broadcast paths carry no generation and keep settling unconditionally
        // — their retry events live under their own path and never collide
        // with a peer-edit delivery.
        let iam_path = "/rustfs/admin/v3/site-replication/peer/iam-item";
        upsert_site_replication_retry_event(&mut queue, &peer, iam_path, "peer offline", None);
        assert_eq!(dequeue_site_replication_retry_events(&mut queue, &peer, iam_path), 1);
    }

    /// P1-15 review follow-up: the receiving side of the ordering fence. Two
    /// nodes of the sending site can fan out in the opposite order to their
    /// commits; the receiver decides ordering from the generation the sender
    /// allocated under the distributed state lock.
    #[test]
    fn peer_edit_fence_rejects_a_delivery_the_newer_edit_already_passed() {
        let mut state = SiteReplicationState::default();
        let path = peer_edit_path_with_fence(Some("origin-site"), 7);
        let queries = query_pairs(&path.parse::<Uri>().expect("the fenced path must be a valid request uri"));
        let (origin, generation) = peer_edit_fence(&queries).expect("the fence must round-trip through the request path");
        assert_eq!((origin.as_str(), generation), ("origin-site", 7));

        assert!(!peer_edit_delivery_is_stale(&state, &origin, generation));
        record_applied_peer_edit_generation(&mut state, &origin, generation);

        // The delivery that lost the race carries the older generation.
        assert!(peer_edit_delivery_is_stale(&state, "origin-site", 6));
        // The generation already applied is NOT stale: one edit fans out one
        // delivery per peer record under a single generation (the ILM-expiry
        // edit), so an equal-generation delivery is the same edit's next body
        // (or an idempotent replay) and must apply.
        assert!(!peer_edit_delivery_is_stale(&state, "origin-site", 7));
        // The next edit from that origin still applies...
        assert!(!peer_edit_delivery_is_stale(&state, "origin-site", 8));
        // ...and another origin site is ordered independently.
        assert!(!peer_edit_delivery_is_stale(&state, "other-site", 1));

        // A sender with no deployment id has nothing to fence against, and a
        // peer that predates the fence sends no query: both keep the previous
        // last-writer-wins behaviour rather than being rejected.
        assert_eq!(peer_edit_path_with_fence(None, 9), SITE_REPLICATION_PEER_EDIT_PATH);
        assert_eq!(peer_edit_path_with_fence(Some(""), 9), SITE_REPLICATION_PEER_EDIT_PATH);
        assert!(peer_edit_fence(&HashMap::new()).is_none());
    }

    /// P1-15 PR2: an accepted join must PRESERVE the peer-edit high-water
    /// marks of peers that stayed. Join fan-outs are routine (every add and
    /// every service-account rotation delivers `SRPeerJoin` to existing
    /// peers), so a blanket reset here would let any stalled older edit from
    /// a peer that never left land after the join and roll its record back —
    /// exactly the interleaving the fence exists to reject.
    #[test]
    fn peer_join_preserves_live_edit_generation_marks() {
        let local = PeerInfo {
            deployment_id: "site-b".to_string(),
            ..peer("site-b", "https://site-b.example.com")
        };
        let remote = PeerInfo {
            deployment_id: "site-a".to_string(),
            ..peer("site-a", "https://site-a.example.com")
        };
        // `remote` is already a peer and has delivered edits up to generation
        // 12; the incoming join (say, a rotation fan-out) keeps both sites.
        let mut state = SiteReplicationState {
            peers: BTreeMap::from([
                (local.deployment_id.clone(), local.clone()),
                (remote.deployment_id.clone(), remote.clone()),
            ]),
            applied_edit_generations: BTreeMap::from([(remote.deployment_id.clone(), 12)]),
            ..Default::default()
        };

        apply_peer_join(
            &mut state,
            &local,
            SRPeerJoinReq {
                svc_acct_access_key: SITE_REPLICATOR_SERVICE_ACCOUNT.to_string(),
                svc_acct_secret_key: "svc-secret".to_string(),
                svc_acct_parent: "root".to_string(),
                peers: BTreeMap::from([
                    (local.deployment_id.clone(), local.clone()),
                    (remote.deployment_id.clone(), remote.clone()),
                ]),
                updated_at: Some(OffsetDateTime::now_utc()),
            },
            true,
        );

        assert_eq!(
            state.applied_edit_generations.get(&remote.deployment_id),
            Some(&12),
            "the join must keep the live mark for a peer that stayed: {:?}",
            state.applied_edit_generations
        );
        assert!(
            peer_edit_delivery_is_stale(&state, &remote.deployment_id, 11),
            "a stalled pre-join delivery must still be fenced out after the join"
        );
        assert_eq!(state.peers.len(), 2, "the join snapshot replaces the local topology");
    }

    /// One edit fans out one delivery per peer record under a single
    /// generation (the ILM-expiry edit sends every peer's record). The
    /// receiver's fenced sequence — staleness check, apply, raise the
    /// high-water mark — must therefore accept every body of that fan-out,
    /// not just the first, while a strictly older delivery stays rejected.
    #[test]
    fn peer_edit_fence_admits_every_body_of_one_edits_fan_out() {
        let local = PeerInfo {
            deployment_id: "site-a".to_string(),
            ..peer("site-a", "https://site-a.example.com")
        };
        let mut state = SiteReplicationState {
            peers: BTreeMap::from([
                ("site-a".to_string(), local.clone()),
                (
                    "site-b".to_string(),
                    PeerInfo {
                        deployment_id: "site-b".to_string(),
                        ..peer("site-b", "https://site-b.example.com")
                    },
                ),
                (
                    "site-c".to_string(),
                    PeerInfo {
                        deployment_id: "site-c".to_string(),
                        ..peer("site-c", "https://site-c.example.com")
                    },
                ),
            ]),
            ..Default::default()
        };
        let origin = "origin-site";
        let generation = 2;

        let bodies: Vec<PeerInfo> = state
            .peers
            .values()
            .map(|peer| PeerInfo {
                replicate_ilm_expiry: true,
                ..peer.clone()
            })
            .collect();
        for body in bodies {
            assert!(
                !peer_edit_delivery_is_stale(&state, origin, generation),
                "a same-generation fan-out body must not be fenced out"
            );
            state = apply_internal_peer_edit(state, &local, body, None).expect("fan-out body applies");
            record_applied_peer_edit_generation(&mut state, origin, generation);
        }

        assert!(
            state.peers.values().all(|peer| peer.replicate_ilm_expiry),
            "every peer record from the fan-out must be applied: {:?}",
            state.peers
        );
        assert!(peer_edit_delivery_is_stale(&state, origin, generation - 1));
    }

    /// P1-15 review follow-up: a site that leaves the mesh drops below two
    /// peers, which clears its state object and restarts its generation
    /// counter at zero. A mark left over from its previous membership would
    /// make every edit it sends after rejoining look stale — i.e. the fence
    /// would silently swallow that site's edits forever.
    #[test]
    fn peer_edit_marks_do_not_outlive_the_peer_that_earned_them() {
        let mut state = SiteReplicationState::default();
        state.peers.insert(
            "origin-site".to_string(),
            PeerInfo {
                deployment_id: "origin-site".to_string(),
                ..peer("origin", "https://origin.example:9000")
            },
        );
        record_applied_peer_edit_generation(&mut state, "origin-site", 12);
        let retained = serde_json::to_vec(&state).expect("serialize state with a live peer");
        assert_eq!(
            parse_site_replication_state(&retained)
                .expect("reload")
                .applied_edit_generations
                .get("origin-site"),
            Some(&12),
            "the mark for a current peer must survive a reload"
        );

        state.peers.remove("origin-site");
        let departed = serde_json::to_vec(&state).expect("serialize state after the peer left");
        let reloaded = parse_site_replication_state(&departed).expect("reload");
        assert!(
            reloaded.applied_edit_generations.is_empty(),
            "a departed peer's mark must not fence its edits after it rejoins: {:?}",
            reloaded.applied_edit_generations
        );
        assert!(!peer_edit_delivery_is_stale(&reloaded, "origin-site", 1));
    }

    #[test]
    fn test_retry_stats_for_state_counts_pending_and_failed() {
        let state = SiteReplicationState {
            retry_queue: vec![
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
            ],
            ..Default::default()
        };

        let stats = retry_stats_for_state(&state).expect("retry stats should be present");

        assert_eq!(stats.pending, 1);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.last_error, "failed");
    }

    #[test]
    fn test_retry_event_dequeue_matches_deployment_id_or_endpoint() {
        let peer = PeerInfo {
            deployment_id: "current-dep".to_string(),
            ..peer("remote", "https://remote.example.com")
        };
        let path = "/rustfs/admin/v3/site-replication/peer/iam-item";
        let mut queue = vec![
            SiteReplicationRetryEvent {
                id: "same-endpoint".to_string(),
                peer_deployment_id: "old-dep".to_string(),
                peer_endpoint: "https://remote.example.com".to_string(),
                path: path.to_string(),
                ..Default::default()
            },
            SiteReplicationRetryEvent {
                id: "different-path".to_string(),
                peer_deployment_id: "old-dep".to_string(),
                peer_endpoint: "https://remote.example.com".to_string(),
                path: "/rustfs/admin/v3/site-replication/peer/bucket-meta".to_string(),
                ..Default::default()
            },
        ];

        let removed = dequeue_site_replication_retry_events(&mut queue, &peer, path);

        assert_eq!(removed, 1);
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].id, "different-path");
    }

    #[test]
    fn test_retry_event_replayed_by_bootstrap_only_clears_replayable_bucket_ops() {
        let retry_event = |id: &str, path: &str| SiteReplicationRetryEvent {
            id: id.to_string(),
            path: path.to_string(),
            ..Default::default()
        };
        let mut queue = vec![
            retry_event(
                "make",
                "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=make-with-versioning",
            ),
            retry_event(
                "configure",
                "/rustfs/admin/v3/site-replication/peer/bucket-ops?operation=configure-replication&bucket=photos",
            ),
            retry_event(
                "delete",
                "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=delete-bucket",
            ),
            retry_event(
                "force-delete",
                "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=force-delete-bucket",
            ),
            retry_event(
                "purge",
                "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=purge-deleted-bucket",
            ),
            retry_event(
                "unknown",
                "/rustfs/admin/v3/site-replication/peer/bucket-ops?bucket=photos&operation=custom",
            ),
            retry_event("iam", "/rustfs/admin/v3/site-replication/peer/iam-item"),
            retry_event("bucket-meta", "/rustfs/admin/v3/site-replication/peer/bucket-meta"),
        ];

        queue.retain(|event| !retry_event_replayed_by_bootstrap(event));

        let retained_ids = queue.iter().map(|event| event.id.as_str()).collect::<Vec<_>>();
        assert_eq!(retained_ids, vec!["delete", "force-delete", "purge", "unknown", "iam", "bucket-meta"]);
    }

    #[test]
    fn test_remove_sites_prunes_retry_queue_for_removed_peer() {
        let state = SiteReplicationState {
            name: "local".to_string(),
            peers: BTreeMap::from([(
                "remote-dep".to_string(),
                PeerInfo {
                    deployment_id: "remote-dep".to_string(),
                    name: "remote".to_string(),
                    endpoint: "https://remote.example.com".to_string(),
                    ..Default::default()
                },
            )]),
            retry_queue: vec![SiteReplicationRetryEvent {
                peer_deployment_id: "remote-dep".to_string(),
                peer_endpoint: "https://remote.example.com".to_string(),
                path: "/rustfs/admin/v3/site-replication/peer/iam-item".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };

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
    fn test_removed_deployment_ids_for_remove_req_uses_pre_remove_state() {
        let state = SiteReplicationState {
            name: "site-c".to_string(),
            peers: BTreeMap::from([
                (
                    "site-a-dep".to_string(),
                    PeerInfo {
                        deployment_id: "site-a-dep".to_string(),
                        name: "site-a".to_string(),
                        ..peer("site-a", "https://site-a.example.com")
                    },
                ),
                (
                    "site-b-dep".to_string(),
                    PeerInfo {
                        deployment_id: "site-b-dep".to_string(),
                        name: "site-b".to_string(),
                        ..peer("site-b", "https://site-b.example.com")
                    },
                ),
                (
                    "site-c-dep".to_string(),
                    PeerInfo {
                        deployment_id: "site-c-dep".to_string(),
                        name: "site-c".to_string(),
                        ..peer("site-c", "https://site-c.example.com")
                    },
                ),
            ]),
            ..Default::default()
        };

        let removed = removed_deployment_ids_for_remove_req(
            &state,
            &SRRemoveReq {
                site_names: vec!["site-b".to_string()],
                ..Default::default()
            },
        );
        assert_eq!(removed, HashSet::from(["site-b-dep".to_string()]));

        let removed_local = removed_deployment_ids_for_remove_req(
            &state,
            &SRRemoveReq {
                site_names: vec!["site-c".to_string()],
                ..Default::default()
            },
        );
        assert_eq!(
            removed_local,
            HashSet::from(["site-a-dep".to_string(), "site-b-dep".to_string(), "site-c-dep".to_string()])
        );
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

    fn site_repl_config(peer: &str) -> ReplicationConfiguration {
        ReplicationConfiguration {
            role: String::new(),
            rules: vec![build_site_replication_rule(
                &format!("arn:rustfs:replication::{peer}:photos"),
                1,
                &format!("site-repl-{peer}"),
            )],
        }
    }

    fn replication_target(deployment_id: &str, endpoint: &str, secret: &str) -> BucketTarget {
        BucketTarget {
            source_bucket: "photos".to_string(),
            target_bucket: "photos".to_string(),
            endpoint: endpoint.to_string(),
            deployment_id: deployment_id.to_string(),
            arn: format!("arn:rustfs:replication::{deployment_id}:photos"),
            target_type: BucketTargetType::ReplicationService,
            credentials: Some(crate::admin::storage_api::bucket::target::Credentials {
                access_key: SITE_REPLICATOR_SERVICE_ACCOUNT.to_string(),
                secret_key: secret.to_string(),
                session_token: None,
                expiration: None,
            }),
            ..Default::default()
        }
    }

    fn state_with_peer(deployment_id: &str, endpoint: &str) -> SiteReplicationState {
        let mut state = SiteReplicationState::default();
        state.peers.insert(
            deployment_id.to_string(),
            PeerInfo {
                deployment_id: deployment_id.to_string(),
                ..peer(deployment_id, endpoint)
            },
        );
        state
    }

    // Bucket targets are writable by anyone holding `admin:SetBucketTarget`. Recovering a
    // secret from a target that merely carries the site-replicator access key would let such
    // a principal choose the secret for the broadly privileged replication account.
    #[test]
    fn test_secret_recovery_rejects_target_outside_the_peer_topology() {
        let state = state_with_peer("remote", "http://remote.example.com:9000");

        assert!(
            bucket_target_matches_configured_peer(
                &replication_target("remote", "remote.example.com:9000", "shared-secret"),
                &state
            ),
            "a target naming a configured peer at its recorded endpoint is ours"
        );
        assert!(
            !bucket_target_matches_configured_peer(
                &replication_target("attacker", "attacker.example.com:9000", "planted-secret"),
                &state
            ),
            "a target naming an unknown deployment must never seed the replication account"
        );
        assert!(
            !bucket_target_matches_configured_peer(
                &replication_target("remote", "attacker.example.com:9000", "planted-secret"),
                &state
            ),
            "a target reusing a peer id but pointing elsewhere must not seed the account"
        );
    }

    // A transient store failure must not be read as "the account is gone" and trigger a
    // reseed that overwrites a live account.
    #[test]
    fn test_only_missing_account_errors_allow_reseeding() {
        use rustfs_iam::error::Error as IamError;

        assert!(is_missing_service_account_error(&IamError::NoSuchAccount("x".into())));
        assert!(is_missing_service_account_error(&IamError::NoSuchServiceAccount("x".into())));
        assert!(is_missing_service_account_error(&IamError::ConfigNotFound));
        assert!(
            !is_missing_service_account_error(&IamError::IAMActionNotAllowed),
            "a permission failure is not evidence that the account is absent"
        );
    }

    fn operator_rule(id: &str) -> ReplicationRule {
        ReplicationRule {
            id: Some(id.to_string()),
            ..build_site_replication_rule("arn:aws:s3:::backup", 1, id)
        }
    }

    // The one-directional bug: the joined site applied the initiator's replication config
    // verbatim, so its own `site-repl-<initiator>` rule was replaced by a rule pointing at
    // itself. No bucket target backs that ARN, so every object was dropped without a log.
    #[test]
    fn test_merge_incoming_replication_config_keeps_local_reverse_rule() {
        let merged = merge_incoming_replication_config(Some(site_repl_config("home")), Some(site_repl_config("office")))
            .expect("merge should keep the local rule");

        assert_eq!(merged.rules.len(), 1);
        assert_eq!(merged.rules[0].id.as_deref(), Some("site-repl-office"));
        assert_eq!(merged.rules[0].destination.bucket, "arn:rustfs:replication::office:photos");
    }

    // A peer deleting its replication config must not delete the receiver's reverse rule
    // either — the delete travels as `replication-config` with no payload.
    #[test]
    fn test_merge_incoming_replication_config_survives_peer_delete() {
        let merged = merge_incoming_replication_config(None, Some(site_repl_config("office")))
            .expect("local site rules must survive a peer delete");

        assert_eq!(merged.rules.len(), 1);
        assert_eq!(merged.rules[0].id.as_deref(), Some("site-repl-office"));
    }

    #[test]
    fn test_merge_incoming_replication_config_replicates_operator_rules() {
        let mut incoming = site_repl_config("home");
        incoming.rules.push(operator_rule("nightly-backup"));
        incoming.role = "arn:rustfs:replication::home:photos".to_string();

        let merged = merge_incoming_replication_config(Some(incoming), Some(site_repl_config("office")))
            .expect("merge should produce rules");

        let ids: Vec<_> = merged.rules.iter().filter_map(|rule| rule.id.as_deref()).collect();
        assert_eq!(ids, vec!["nightly-backup", "site-repl-office"]);
        assert_eq!(merged.rules[0].priority, Some(1));
        assert_eq!(merged.rules[1].priority, Some(2));
        assert!(
            merged.role.is_empty(),
            "a site-replication ARN in `role` belongs to the sender and must not be adopted"
        );
    }

    #[test]
    fn test_merge_incoming_replication_config_returns_none_when_nothing_remains() {
        assert!(merge_incoming_replication_config(Some(site_repl_config("home")), None).is_none());
    }

    // `role` is part of the bucket's S3-visible configuration. Repairing a reverse rule must
    // drop only a sender-owned site-replication ARN, never an operator's own role — the same
    // rule the merge path applies, so both paths agree on what is ours to rewrite.
    #[test]
    fn test_replication_role_is_only_cleared_when_it_is_a_site_replication_arn() {
        let operator_role = "arn:aws:iam::123456789012:role/replication";
        assert!(
            replication_target_arn_deployment_id(operator_role).is_none(),
            "an operator IAM role is not a site-replication ARN and must be preserved"
        );
        assert_eq!(
            replication_target_arn_deployment_id("arn:rustfs:replication::home:photos").as_deref(),
            Some("home"),
            "a site-replication ARN is sender-owned and gets cleared"
        );

        let mut incoming = site_repl_config("home");
        incoming.role = operator_role.to_string();
        let merged = merge_incoming_replication_config(Some(incoming), Some(site_repl_config("office")))
            .expect("merge should produce rules");
        assert_eq!(merged.role, operator_role, "operator role must survive the merge");
    }

    // Rules and targets are keyed off the same ARN. Minting a fresh one while
    // `reconcile_site_replication_bucket_targets` preserves a MinIO-era `arn:minio:...`
    // target would leave the rule pointing at an ARN no target satisfies.
    #[test]
    fn test_build_site_replication_config_reuses_configured_arn() {
        let mut state = SiteReplicationState {
            service_account_access_key: "site-replicator-0".to_string(),
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
        let existing = ReplicationConfiguration {
            role: String::new(),
            rules: vec![build_site_replication_rule(
                "arn:minio:replication::remote:photos",
                1,
                "site-repl-remote",
            )],
        };

        let config = build_site_replication_config(
            "photos",
            &state,
            &PeerInfo {
                deployment_id: "local".to_string(),
                ..peer("local", "https://local.example.com")
            },
            "runtime-iam-secret",
            Some(&existing),
        )
        .expect("build site replication config")
        .expect("a remote peer yields one rule");

        assert_eq!(config.rules.len(), 1);
        assert_eq!(config.rules[0].destination.bucket, "arn:minio:replication::remote:photos");
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

        let regional_arn = "arn:rustfs:replication:eu-west-1:remote:photos";
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![build_site_replication_rule(regional_arn, 1, "site-repl-remote")],
        };
        state.peers.get_mut("remote").expect("remote peer should exist").endpoint = "http://moved.example.com:9001".to_string();
        let targets = reconcile_site_replication_bucket_targets(
            targets,
            "photos",
            &state,
            &PeerInfo {
                deployment_id: "local".to_string(),
                ..peer("local", "https://local.example.com")
            },
            Some(&config),
            "runtime-iam-secret",
        )
        .expect("reconcile moved peer target");

        assert_eq!(targets.targets.len(), 1);
        assert_eq!(targets.targets[0].endpoint, "moved.example.com:9001");
        assert_eq!(targets.targets[0].arn, regional_arn);
        assert_eq!(
            replication_target_arn_deployment_id("arn:minio:replication:eu-west-1:remote:photos").as_deref(),
            Some("remote")
        );

        let retry = PeerInfo {
            deployment_id: "remote".to_string(),
            endpoint: "https://moved.example.com:9001".to_string(),
            ..Default::default()
        };
        assert!(peer_endpoint_edit_requested(&state, &retry));
        state.peers.get_mut("remote").expect("remote peer should exist").endpoint = retry.endpoint;
        let targets = reconcile_site_replication_bucket_targets(
            targets,
            "photos",
            &state,
            &PeerInfo {
                deployment_id: "local".to_string(),
                ..peer("local", "https://local.example.com")
            },
            Some(&config),
            "runtime-iam-secret",
        )
        .expect("reconcile secure peer target");

        assert_eq!(targets.targets.len(), 1);
        assert_eq!(targets.targets[0].endpoint, "moved.example.com:9001");
        assert_eq!(targets.targets[0].arn, regional_arn);
        assert!(targets.targets[0].secure);

        let mut mismatched = PeerInfo {
            deployment_id: "source-view-remote".to_string(),
            name: "remote".to_string(),
            endpoint: "https://moved.example.com:9001".to_string(),
            ..Default::default()
        };
        align_peer_edit_deployment_id(&state, &mut mismatched);
        assert_eq!(mismatched.deployment_id, "remote");

        let retry_peer = PeerInfo {
            deployment_id: "remote".to_string(),
            endpoint: "https://moved.example.com:9001".to_string(),
            ..Default::default()
        };
        assert!(!peer_endpoint_refresh_requested(&state, &retry_peer));

        let mut ambiguous_state = state.clone();
        ambiguous_state.peers.insert(
            "remote-duplicate".to_string(),
            PeerInfo {
                deployment_id: "remote-duplicate".to_string(),
                name: "remote".to_string(),
                endpoint: "https://duplicate.example.com:9001".to_string(),
                ..Default::default()
            },
        );
        let mut ambiguous = mismatched.clone();
        ambiguous.deployment_id = "source-view-remote".to_string();
        align_peer_edit_deployment_id(&ambiguous_state, &mut ambiguous);
        assert_eq!(ambiguous.deployment_id, "source-view-remote");

        let remote_peers = state.peers.clone();
        set_pending_endpoint_refresh(
            &mut state,
            PendingEndpointRefresh {
                id: "refresh-1".to_string(),
                peer: retry_peer.clone(),
                remote_peers,
                acked_deployment_ids: BTreeSet::new(),
            },
        )
        .expect("set pending endpoint refresh");
        assert!(peer_endpoint_refresh_requested(&state, &retry_peer));
        state.pending_endpoint_refresh = None;
        assert!(pending_endpoint_refresh(&state).is_none());
        clear_pending_endpoint_refresh(&mut state);
        assert!(pending_endpoint_refresh(&state).is_none());
        assert!(!peer_endpoint_refresh_requested(&state, &retry_peer));
        assert!(parse_endpoint_refresh_status(&mismatched, b"").is_err());
        assert!(parse_endpoint_refresh_status(&mismatched, br#"{"success":false,"errorDetail":"refresh failed"}"#).is_err());
        assert!(parse_endpoint_refresh_status(&mismatched, br#"{"success":true}"#).is_ok());
        assert!(
            endpoint_refresh_capability_supported(&mismatched, StatusCode::OK, br#"{"success":true}"#)
                .expect("current peer capability response")
        );
        assert!(!endpoint_refresh_capability_supported(&mismatched, StatusCode::OK, b"").expect("legacy empty response"));
        assert!(
            !endpoint_refresh_capability_supported(&mismatched, StatusCode::BAD_REQUEST, b"unsupported")
                .expect("legacy bad request response")
        );
        assert!(endpoint_refresh_capability_supported(&mismatched, StatusCode::UNAUTHORIZED, b"denied").is_err());

        let old_target = PeerInfo {
            deployment_id: "remote".to_string(),
            endpoint: "http://old.example.com:9000".to_string(),
            ..Default::default()
        };
        let pending = PendingEndpointRefresh {
            id: "refresh-2".to_string(),
            peer: PeerInfo {
                deployment_id: "remote".to_string(),
                endpoint: "https://new.example.com:9001".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        let route_endpoints = endpoint_refresh_route_endpoints(&old_target, &pending)
            .expect("endpoint refresh routes")
            .into_iter()
            .map(|connection| connection.endpoint().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            route_endpoints,
            vec![
                "http://old.example.com:9000".to_string(),
                "https://new.example.com:9001".to_string()
            ]
        );

        let tls_changed = PendingEndpointRefresh {
            peer: PeerInfo {
                deployment_id: "remote".to_string(),
                endpoint: "https://same.example.com".to_string(),
                skip_tls_verify: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let old_tls_target = PeerInfo {
            deployment_id: "remote".to_string(),
            endpoint: "https://same.example.com".to_string(),
            ..Default::default()
        };
        assert_eq!(
            endpoint_refresh_route_endpoints(&old_tls_target, &tls_changed)
                .expect("TLS-only endpoint refresh routes")
                .len(),
            2
        );
        let routing_peers = BTreeMap::from([
            (
                "local".to_string(),
                PeerInfo {
                    deployment_id: "local".to_string(),
                    ..Default::default()
                },
            ),
            ("remote".to_string(), old_target),
        ]);
        let mut acked_pending = pending.clone();
        acked_pending.acked_deployment_ids.insert("remote".to_string());
        assert!(endpoint_refresh_remote_targets(&routing_peers, Some(&acked_pending), Some("local")).is_empty());

        let mut request = serde_json::to_value(EndpointRefreshRequest {
            id: pending.id,
            peer: pending.peer,
        })
        .expect("serialize endpoint refresh request");
        request
            .as_object_mut()
            .expect("endpoint refresh request object")
            .insert("unexpected".to_string(), Value::Bool(true));
        assert!(serde_json::from_value::<EndpointRefreshRequest>(request).is_err());
        assert_eq!(
            peer_bucket_names_from_metainfo("https://minio.example.com", br#"{"Buckets":{"archive":{},"photos":{}}}"#)
                .expect("MinIO metainfo bucket inventory"),
            vec!["archive".to_string(), "photos".to_string()]
        );
        assert_eq!(
            peer_bucket_names_from_metainfo("https://rustfs.example.com", br#"{"buckets":{"photos":{}}}"#)
                .expect("RustFS metainfo bucket inventory"),
            vec!["photos".to_string()]
        );
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
            "/minio/admin/v3/site-replication/peer/join"
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
        assert!(site_replication_peer_payload_encrypted("/minio/admin/v3/site-replication/peer/join"));
        assert!(site_replication_peer_payload_encrypted(
            "/minio/admin/v3/site-replication/peer/join?bootstrapToken=token"
        ));
        // The outbound rewrite no longer produces the legacy `/site-replication/join`
        // path; it must not be treated as an encrypted MinIO route.
        assert!(!site_replication_peer_payload_encrypted("/minio/admin/v3/site-replication/join"));
        assert!(!site_replication_peer_payload_encrypted(
            "/minio/admin/v3/site-replication/peer/bucket-meta"
        ));
        assert!(!site_replication_peer_payload_encrypted("/minio/admin/v3/site-replication/peer/iam-item"));
    }

    #[test]
    fn test_parse_peer_join_response_tolerates_empty_minio_success_body() {
        let fallback = PeerInfo {
            deployment_id: "remote-deployment".to_string(),
            ..peer("remote", "https://remote.example.com")
        };

        for body in [&b""[..], b" \r\n\t "] {
            let response = parse_peer_join_response(body, fallback.clone()).expect("empty join body is a MinIO success");
            assert_eq!(response.peer.deployment_id, "remote-deployment");
            assert_eq!(response.peer.endpoint, "https://remote.example.com");
            assert!(response.initial_sync_error_message.is_empty());
        }

        let json = serde_json::to_vec(&SRPeerJoinResponse {
            peer: peer("actual", "https://actual.example.com"),
            initial_sync_error_message: "sync failed".to_string(),
        })
        .expect("serialize join response");
        let response = parse_peer_join_response(&json, fallback.clone()).expect("parse join response body");
        assert_eq!(response.peer.endpoint, "https://actual.example.com");
        assert_eq!(response.initial_sync_error_message, "sync failed");

        assert!(parse_peer_join_response(b"not-json", fallback).is_err());
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
        let previous_generation = current_outbound_tls_generation().0;
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
    fn test_site_repl_netperf_reports_unsupported_without_measurements() {
        let result = unsupported_site_netperf_result("https://peer.example.com".to_string());

        assert_eq!(result.endpoint, "https://peer.example.com");
        assert_eq!(result.tx, 0);
        assert_eq!(result.rx, 0);
        assert_eq!(result.total_conn, 0);
        assert!(result.error.contains("unsupported"));
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
    fn test_gob_site_netperf_unsupported_error_matches_go_encoding() {
        let data =
            encode_go_gob_site_netperf_node_result(&unsupported_site_netperf_result("https://peer.example.com".to_string()));

        // Generated independently with Go's encoding/gob Encoder from the
        // MinIO-compatible SiteNetPerfNodeResult shape. This specifically
        // covers the field delta from Endpoint to Error when all counters are zero.
        let expected: &[u8] = &[
            0x7d, 0x7f, 0x03, 0x01, 0x01, 0x15, 0x53, 0x69, 0x74, 0x65, 0x4e, 0x65, 0x74, 0x50, 0x65, 0x72, 0x66, 0x4e, 0x6f,
            0x64, 0x65, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x01, 0xff, 0x80, 0x00, 0x01, 0x07, 0x01, 0x08, 0x45, 0x6e, 0x64,
            0x70, 0x6f, 0x69, 0x6e, 0x74, 0x01, 0x0c, 0x00, 0x01, 0x02, 0x54, 0x58, 0x01, 0x06, 0x00, 0x01, 0x0f, 0x54, 0x58,
            0x54, 0x6f, 0x74, 0x61, 0x6c, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x01, 0x04, 0x00, 0x01, 0x02, 0x52,
            0x58, 0x01, 0x06, 0x00, 0x01, 0x0f, 0x52, 0x58, 0x54, 0x6f, 0x74, 0x61, 0x6c, 0x44, 0x75, 0x72, 0x61, 0x74, 0x69,
            0x6f, 0x6e, 0x01, 0x04, 0x00, 0x01, 0x09, 0x54, 0x6f, 0x74, 0x61, 0x6c, 0x43, 0x6f, 0x6e, 0x6e, 0x01, 0x06, 0x00,
            0x01, 0x05, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x01, 0x0c, 0x00, 0x00, 0x00, 0x73, 0xff, 0x80, 0x01, 0x18, 0x68, 0x74,
            0x74, 0x70, 0x73, 0x3a, 0x2f, 0x2f, 0x70, 0x65, 0x65, 0x72, 0x2e, 0x65, 0x78, 0x61, 0x6d, 0x70, 0x6c, 0x65, 0x2e,
            0x63, 0x6f, 0x6d, 0x06, 0x54, 0x73, 0x69, 0x74, 0x65, 0x2d, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69,
            0x6f, 0x6e, 0x20, 0x6e, 0x65, 0x74, 0x70, 0x65, 0x72, 0x66, 0x20, 0x69, 0x73, 0x20, 0x75, 0x6e, 0x73, 0x75, 0x70,
            0x70, 0x6f, 0x72, 0x74, 0x65, 0x64, 0x20, 0x62, 0x65, 0x63, 0x61, 0x75, 0x73, 0x65, 0x20, 0x52, 0x75, 0x73, 0x74,
            0x46, 0x53, 0x20, 0x64, 0x6f, 0x65, 0x73, 0x20, 0x6e, 0x6f, 0x74, 0x20, 0x70, 0x65, 0x72, 0x66, 0x6f, 0x72, 0x6d,
            0x20, 0x70, 0x65, 0x65, 0x72, 0x20, 0x74, 0x72, 0x61, 0x66, 0x66, 0x69, 0x63, 0x00,
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
        let site_config_xml = |peer: &str| {
            let config = ReplicationConfiguration {
                role: String::new(),
                rules: vec![build_site_replication_rule(
                    &format!("arn:rustfs:replication::{peer}:bucket"),
                    1,
                    &format!("site-repl-{peer}"),
                )],
            };
            String::from_utf8(serialize(&config).unwrap()).unwrap()
        };
        let dep_a_xml = site_config_xml("dep-b");
        let dep_b_xml = site_config_xml("dep-a");

        // Peer that has complete config for 2-site setup
        assert!(site_replication_rule_complete(
            &build_site_replication_rule("arn:rustfs:replication::dep-b:bucket", 1, "site-repl-dep-b"),
            "dep-a"
        ));
        assert_eq!(
            site_replication_config_mismatch(vec![("dep-a", Some(&dep_a_xml)), ("dep-b", Some(&dep_b_xml))].into_iter(), 2),
            (2, false),
            "complete rules on both sites → no mismatch"
        );
        assert_eq!(
            site_replication_config_mismatch(vec![("dep-a", Some(&dep_a_xml)), ("dep-b", None)].into_iter(), 2),
            (1, true),
            "config only on one of two sites → mismatch"
        );
    }

    // Status miscount regression: build_sr_info stores replication_config as base64-encoded XML
    // (the wire form). site_replication_config_mismatch must decode it before XML-parsing; before
    // the fix it parsed the base64 text directly, always failed, and reported every replicated
    // bucket as out-of-sync ("0/N Buckets in sync"). This test feeds the real base64 wire form.
    #[test]
    fn test_site_replication_config_mismatch_accepts_base64_wire_form() {
        let site_config_xml = |peer: &str| {
            let config = ReplicationConfiguration {
                role: String::new(),
                rules: vec![build_site_replication_rule(
                    &format!("arn:rustfs:replication::{peer}:bucket"),
                    1,
                    &format!("site-repl-{peer}"),
                )],
            };
            String::from_utf8(serialize(&config).unwrap()).unwrap()
        };
        let dep_a_xml = site_config_xml("dep-b");
        let dep_b_xml = site_config_xml("dep-a");
        let dep_a_b64 = BASE64_STANDARD.encode(dep_a_xml.as_bytes());
        let dep_b_b64 = BASE64_STANDARD.encode(dep_b_xml.as_bytes());

        // Both sites present the complete config in base64 wire form → NOT a mismatch.
        assert_eq!(
            site_replication_config_mismatch(vec![("dep-a", Some(&dep_a_b64)), ("dep-b", Some(&dep_b_b64))].into_iter(), 2),
            (2, false),
            "base64-encoded complete configs on both sites must not be reported as a mismatch"
        );
        // The tolerant decode keeps plain-XML callers working too.
        assert_eq!(
            site_replication_config_mismatch(vec![("dep-a", Some(&dep_a_xml)), ("dep-b", Some(&dep_b_xml))].into_iter(), 2),
            (2, false),
            "raw-XML wire form still parses via the base64 fallback"
        );
        // A base64 config present on only one of two sites is still a mismatch.
        assert_eq!(
            site_replication_config_mismatch(vec![("dep-a", Some(&dep_a_b64)), ("dep-b", None)].into_iter(), 2),
            (1, true),
            "config present on only one site is a mismatch regardless of encoding"
        );
    }

    // BUG1: peers persisted on add/join must carry a real sync_state (Enable), not Unknown,
    // so `mc admin replicate info` and the console show the correct state for healthy peers.
    #[test]
    fn test_added_peers_persist_enable_sync_state() {
        let local = peer("local", "https://local.example.com");
        let sites = vec![PeerSite {
            name: "remote".to_string(),
            endpoint: "https://remote.example.com".to_string(),
            ..Default::default()
        }];
        let mut peers = build_join_peers(&SiteReplicationState::default(), &local, sites, false);
        // Construction defaults every peer to Unknown — the pre-fix behavior that made
        // `replicate info` render a blank/Unknown Sync column.
        assert!(
            peers.values().all(|p| p.sync_state == SyncStatus::Unknown),
            "freshly constructed peers default to Unknown"
        );
        mark_unknown_peer_sync_enabled(&mut peers);
        assert!(
            !peers.is_empty() && peers.values().all(|p| p.sync_state == SyncStatus::Enable),
            "add/join must persist Enable so the info endpoint reports a real sync state"
        );
    }

    // BUG1: an explicit Disable is a meaningful state and must survive the Unknown -> Enable promotion.
    #[test]
    fn test_mark_peers_sync_enabled_preserves_disable() {
        let mut peers = BTreeMap::new();
        peers.insert(
            "a".to_string(),
            PeerInfo {
                deployment_id: "a".to_string(),
                sync_state: SyncStatus::Unknown,
                ..peer("a", "https://a.example.com")
            },
        );
        peers.insert(
            "b".to_string(),
            PeerInfo {
                deployment_id: "b".to_string(),
                sync_state: SyncStatus::Disable,
                ..peer("b", "https://b.example.com")
            },
        );
        mark_unknown_peer_sync_enabled(&mut peers);
        assert_eq!(peers["a"].sync_state, SyncStatus::Enable, "Unknown must be promoted to Enable");
        assert_eq!(peers["b"].sync_state, SyncStatus::Disable, "explicit Disable must be preserved");
    }

    #[test]
    fn test_join_peer_sync_state_waits_for_deferred_commit() {
        let mut peers = BTreeMap::from([("a".to_string(), peer("a", "https://a.example.com"))]);

        initialize_join_peer_sync_state(&mut peers, true);
        assert_eq!(peers["a"].sync_state, SyncStatus::Unknown);

        initialize_join_peer_sync_state(&mut peers, false);
        assert_eq!(peers["a"].sync_state, SyncStatus::Enable);
    }

    #[test]
    fn test_join_deferred_sync_state_flag_is_wire_compatible() {
        let legacy: SRPeerJoinEnvelope = serde_json::from_value(serde_json::json!({})).expect("parse legacy peer join request");
        assert!(!legacy.defer_sync_state_enable);

        let value = serde_json::to_value(SRPeerJoinEnvelope {
            defer_sync_state_enable: true,
            ..Default::default()
        })
        .expect("serialize peer join request");
        assert_eq!(value.get("deferSyncStateEnable"), Some(&Value::Bool(true)));
    }

    // BUG2: pre-existing-bucket back-fill failures must be surfaced in the add response's
    // initial_sync_error_message, not swallowed behind an unqualified success.
    #[test]
    fn test_initial_sync_error_message_surfaces_backfill_failures() {
        let bootstrap_errors = vec!["peer-x: metadata sync failed".to_string()];
        let backfill_errors = vec![
            "test78787: replication setup skipped (site replication runtime unavailable)".to_string(),
            "test78787 -> https://peer.example.com: resync kick failed: timeout".to_string(),
        ];
        let mut errors = SiteReplicationErrorSummary::default();
        for error in bootstrap_errors.into_iter().chain(backfill_errors) {
            errors.push(error);
        }
        let msg = errors.render();
        assert!(msg.contains("peer-x: metadata sync failed"), "bootstrap errors must be surfaced");
        assert!(
            msg.contains("test78787: replication setup skipped"),
            "a back-fill setup-skip must be surfaced so a dropped bucket is visible"
        );
        assert!(msg.contains("resync kick failed"), "resync kick failures must be surfaced");
    }

    #[test]
    fn test_initial_sync_error_summary_is_bounded() {
        let mut errors = SiteReplicationErrorSummary::default();
        for index in 0..(SITE_REPLICATION_INITIAL_SYNC_ERROR_LIMIT + 5) {
            errors.push(format!("bucket-{index}: {}", "x".repeat(SITE_REPLICATION_PEER_ERROR_DETAIL_LIMIT + 32)));
        }

        let message = errors.render();

        assert_eq!(errors.reported(), SITE_REPLICATION_INITIAL_SYNC_ERROR_LIMIT);
        assert!(message.contains("5 additional error(s) omitted"));
        assert!(message.chars().count() <= SITE_REPLICATION_INITIAL_SYNC_ERROR_LIMIT * 258 + 64);
    }

    #[test]
    fn test_peer_join_response_error_summary_is_wire_compatible() {
        let response: SRPeerJoinResponse = serde_json::from_value(serde_json::json!({
            "peer": peer("remote", "https://remote.example.com")
        }))
        .expect("parse legacy peer join response");

        assert!(response.initial_sync_error_message.is_empty());

        let value = serde_json::to_value(SRPeerJoinResponse {
            peer: peer("remote", "https://remote.example.com"),
            initial_sync_error_message: "bucket setup failed".to_string(),
        })
        .expect("serialize peer join response");
        assert_eq!(value.get("initialSyncErrorMessage").and_then(Value::as_str), Some("bucket setup failed"));
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

    #[test]
    fn site_resync_summary_reports_partial_failure_and_clamps_counters() {
        let now = OffsetDateTime::now_utc();
        let mut running = ResyncBucketStatus {
            bucket: "b".to_string(),
            target_arn: "arn-b".to_string(),
            ..Default::default()
        };
        apply_site_resync_target_status(
            &mut running,
            &replication::TargetReplicationResyncStatus {
                resync_status: replication::ResyncStatusType::ResyncStarted,
                resync_id: "run-1".to_string(),
                replicated_count: 4,
                replicated_size: 16,
                failed_count: -1,
                failed_size: -2,
                ..Default::default()
            },
        );
        let failed = ResyncBucketStatus {
            bucket: "a".to_string(),
            target_arn: "arn-a".to_string(),
            status: "conflict".to_string(),
            err_detail: "durable failure".to_string(),
            ..Default::default()
        };
        let mut status = SRResyncOpStatus {
            op_type: SITE_REPL_RESYNC_START.to_string(),
            resync_id: "run-1".to_string(),
            buckets: vec![running, failed],
            ..Default::default()
        };

        summarize_site_resync_status(&mut status, now);

        assert_eq!(status.status, "failed");
        assert_eq!(status.state, "running");
        assert_eq!(status.running_buckets, 1);
        assert_eq!(status.failed_buckets, 1);
        assert_eq!(status.replicated_objects, 4);
        assert_eq!(status.replicated_bytes, 16);
        assert_eq!(status.failed_objects, 0);
        assert_eq!(status.failed_bytes, 0);
        assert_eq!(status.completed_at, None);
        assert!(site_resync_is_active(&status));
        assert!(site_resync_cancel_is_idempotent(&SRResyncOpStatus {
            state: "canceled".to_string(),
            ..Default::default()
        }));
        assert!(site_bucket_resync_is_active(replication::ResyncStatusType::ResyncPending));
        assert!(site_bucket_resync_is_active(replication::ResyncStatusType::ResyncStarted));
        assert!(!site_bucket_resync_is_active(replication::ResyncStatusType::ResyncCompleted));
    }

    #[test]
    fn site_resync_pagination_is_sorted_and_rejects_stale_cursor() {
        let status = SRResyncOpStatus {
            resync_id: "run-1".to_string(),
            generation: 3,
            buckets: ["a", "b", "c"]
                .into_iter()
                .map(|bucket| ResyncBucketStatus {
                    bucket: bucket.to_string(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let first = site_resync_page(&status, 2, 0).expect("first page should be valid");
        assert!(first.truncated);
        assert_eq!(first.buckets.iter().map(|bucket| bucket.bucket.as_str()).collect::<Vec<_>>(), ["a", "b"]);

        let query = HashMap::from([
            ("limit".to_string(), "2".to_string()),
            ("continuationToken".to_string(), first.next_continuation_token),
        ]);
        let (_, offset) = parse_site_resync_page(&query, &status).expect("cursor should match operation");
        assert_eq!(offset, 2);
        let mut newer = status;
        newer.generation += 1;
        assert!(parse_site_resync_page(&query, &newer).is_err());
    }

    /// P1-15 PR2: the persist-or-skip side of the transaction. A miss
    /// (`StateCommit::Unchanged`) must not write at all: the shared persist
    /// helper clears the whole object once a state has ≤1 peer and no pending
    /// rotation/removal, so a no-op ack or clear that "harmlessly" persisted
    /// would delete the retry queue and every other field along with it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_missed_pending_clear_must_not_rewrite_the_state_object() {
        publish_ready_iam_context().await;

        // One peer, no pending records: exactly the shape the persist helper's clear
        // branch fires on. Only the test-only seeder can write it.
        let seed = SiteReplicationState {
            peers: BTreeMap::from([(
                "site-solo".to_string(),
                PeerInfo {
                    deployment_id: "site-solo".to_string(),
                    ..peer("site-solo", "https://solo.example:9000")
                },
            )]),
            retry_queue: vec![SiteReplicationRetryEvent {
                id: "evt-1".to_string(),
                peer_deployment_id: "site-gone".to_string(),
                peer_endpoint: "https://gone.example:9000".to_string(),
                path: "/rustfs/admin/v3/site-replication/peer/iam-item".to_string(),
                retry_count: 2,
                failed: false,
                last_error: "peer offline".to_string(),
                updated_at: Some(OffsetDateTime::now_utc()),
                edit_generation: None,
            }],
            ..Default::default()
        };
        save_site_replication_state(&seed).await.expect("seed state");

        clear_pending_remove("no-such-remove").await.expect("no-op clear");
        mark_pending_rotation_peer_acked("no-such-rotation", "site-x")
            .await
            .expect("no-op ack");
        record_pending_remove_secret_candidate("no-such-remove", "secret".to_string())
            .await
            .expect("no-op candidate");

        let reloaded = load_site_replication_state().await.expect("reload");
        assert_eq!(
            reloaded.retry_queue.len(),
            1,
            "a missed pending lookup persisted (and therefore cleared) the state object"
        );
        assert_eq!(reloaded.peers.len(), 1, "the peer record must survive the no-op calls");
    }

    /// Review follow-up on P1-15 PR2 (overtrue): two joins accepted by the
    /// same node must not interleave their IAM writes with each other's
    /// commits. Join A loads a stale snapshot and pauses before its IAM
    /// write; join B (newer) applies secret B and commits; A resumes, its
    /// IAM write would overwrite secret B, and its commit is then refused as
    /// superseded — the persisted state advertises B's contract while IAM
    /// holds A's secret. `admit_peer_join` closes this by serializing the
    /// whole admission under the lifecycle guard and re-checking staleness
    /// BEFORE the IAM step: with the guard, B cannot even start while A is
    /// gated mid-IAM. Remove the guard (or move the IAM step ahead of the
    /// fresh staleness check) and this test deadlocks or records B's IAM
    /// write before A finishes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_peer_join_admission_serializes_iam_apply_against_a_newer_join() {
        publish_ready_iam_context().await;

        // Whole-second timestamps so the RFC3339 round trip through the state
        // object cannot lose sub-second precision under the equality asserts.
        let now = OffsetDateTime::now_utc().replace_nanosecond(0).expect("truncate nanos");
        let local = PeerInfo {
            deployment_id: "site-local".to_string(),
            ..peer("site-local", "https://local.example:9000")
        };
        let remote = PeerInfo {
            deployment_id: "site-remote".to_string(),
            ..peer("site-remote", "https://remote.example:9000")
        };
        let seed = SiteReplicationState {
            peers: BTreeMap::from([
                (local.deployment_id.clone(), local.clone()),
                (remote.deployment_id.clone(), remote.clone()),
            ]),
            updated_at: Some(now - Duration::from_secs(60)),
            ..Default::default()
        };
        save_site_replication_state(&seed).await.expect("seed state");

        let join_peers = BTreeMap::from([
            (local.deployment_id.clone(), local.clone()),
            (remote.deployment_id.clone(), remote.clone()),
        ]);
        let join_req = |updated_at: OffsetDateTime, secret: &str| SRPeerJoinReq {
            svc_acct_access_key: "svc-join".to_string(),
            svc_acct_secret_key: secret.to_string(),
            svc_acct_parent: "root".to_string(),
            peers: join_peers.clone(),
            updated_at: Some(updated_at),
        };

        let iam_log: Arc<StdMutex<Vec<&'static str>>> = Arc::new(StdMutex::new(Vec::new()));
        let (a_entered_tx, a_entered_rx) = tokio::sync::oneshot::channel();
        let (a_gate_tx, a_gate_rx) = tokio::sync::oneshot::channel::<()>();

        // Join A (older, T1): pauses inside its IAM step.
        let log_a = iam_log.clone();
        let endpoint_a = "https://local.example:9000".to_string();
        let req_a = join_req(now - Duration::from_secs(30), "secret-a");
        let join_a = tokio::spawn(async move {
            admit_peer_join(endpoint_a, req_a, true, move |_req| async move {
                let _ = a_entered_tx.send(());
                let _ = a_gate_rx.await;
                log_a.lock().expect("iam log").push("iam-a");
                Ok(())
            })
            .await
        });
        a_entered_rx.await.expect("join A reached its IAM step");

        // Join B (newer, T2) arrives while A is gated mid-IAM. The lifecycle
        // guard must hold it at the door.
        let log_b = iam_log.clone();
        let endpoint_b = "https://local.example:9000".to_string();
        let req_b = join_req(now, "secret-b");
        let join_b = tokio::spawn(async move {
            admit_peer_join(endpoint_b, req_b, true, move |_req| async move {
                log_b.lock().expect("iam log").push("iam-b");
                Ok(())
            })
            .await
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            iam_log.lock().expect("iam log").is_empty(),
            "join B ran its IAM step while join A was still mid-admission: {:?}",
            iam_log.lock().expect("iam log")
        );

        a_gate_tx.send(()).expect("release join A");
        let outcome_a = join_a.await.expect("join A task").expect("join A admission");
        let outcome_b = join_b.await.expect("join B task").expect("join B admission");
        assert!(matches!(outcome_a, PeerJoinOutcome::Applied(..)), "join A must commit first");
        assert!(
            matches!(outcome_b, PeerJoinOutcome::Applied(..)),
            "the newer join B must still apply after A"
        );
        assert_eq!(
            *iam_log.lock().expect("iam log"),
            vec!["iam-a", "iam-b"],
            "IAM writes must land in admission order, ending on the committed join's secret"
        );
        assert_eq!(
            load_site_replication_state().await.expect("reload").updated_at,
            Some(now),
            "the persisted state must end on join B, matching the last IAM write"
        );
    }

    /// P1-15 PR2: the three-way contract of `finalize_pending_rotation_if_complete`
    /// — no pending means "already finalized" (true, nothing written), a
    /// different or incomplete rotation is left alone (false), and a fully
    /// acked rotation is cleared in the same transaction that reports true.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial]
    async fn test_finalize_pending_rotation_three_way_contract() {
        publish_ready_iam_context().await;

        let local_peer = PeerInfo {
            deployment_id: "site-local".to_string(),
            ..peer("site-local", "https://local.example:9000")
        };
        let remote_peer = PeerInfo {
            deployment_id: "site-remote".to_string(),
            ..peer("site-remote", "https://remote.example:9000")
        };
        let seed = SiteReplicationState {
            peers: BTreeMap::from([
                (local_peer.deployment_id.clone(), local_peer.clone()),
                (remote_peer.deployment_id.clone(), remote_peer.clone()),
            ]),
            pending_rotation: Some(PendingRotation {
                id: "rot-final".to_string(),
                access_key: "svc-account".to_string(),
                peers: BTreeMap::from([
                    (local_peer.deployment_id.clone(), local_peer.clone()),
                    (remote_peer.deployment_id.clone(), remote_peer.clone()),
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };
        save_site_replication_state(&seed).await.expect("seed state");

        assert!(
            !finalize_pending_rotation_if_complete("other-rotation", &local_peer)
                .await
                .expect("mismatched id"),
            "a different rotation id must not finalize"
        );
        assert!(
            !finalize_pending_rotation_if_complete("rot-final", &local_peer)
                .await
                .expect("incomplete acks"),
            "an un-acked remote peer must block finalization"
        );
        assert!(
            load_site_replication_state()
                .await
                .expect("reload")
                .pending_rotation
                .is_some(),
            "the pending rotation must survive both refusals"
        );

        mark_pending_rotation_peer_acked("rot-final", &remote_peer.deployment_id)
            .await
            .expect("ack remote");
        assert!(
            finalize_pending_rotation_if_complete("rot-final", &local_peer)
                .await
                .expect("finalize"),
            "a fully acked rotation must finalize"
        );
        assert!(
            load_site_replication_state()
                .await
                .expect("reload")
                .pending_rotation
                .is_none(),
            "finalization must clear the pending rotation"
        );
        assert!(
            finalize_pending_rotation_if_complete("rot-final", &local_peer)
                .await
                .expect("idempotent"),
            "no pending rotation means already finalized"
        );
    }

    /// P1-15 review follow-up: isolates the DISTRIBUTED guard, which is now
    /// the whole boundary. Two "nodes" run the production transaction
    /// concurrently; only the state-object write lock keeps their
    /// read-modify-write sequences apart — drop it and this test loses an
    /// update.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_state_object_lock_serializes_writers_from_separate_nodes() {
        publish_ready_iam_context().await;

        let seed = SiteReplicationState {
            pending_rotation: Some(PendingRotation {
                id: "rot-nodes".to_string(),
                access_key: "svc-account".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        save_site_replication_state(&seed).await.expect("seed state");

        // Each "node" runs the production transaction; the distributed
        // state-object lock is the only thing keeping them apart.
        fn node_local_update(candidate: String) -> impl std::future::Future<Output = S3Result<()>> {
            update_site_replication_state(move |state| {
                if let Some(pending) = state.pending_rotation.as_mut() {
                    pending.secret_candidates.push(candidate);
                }
                Ok(())
            })
        }

        const ROUNDS: usize = 8;
        for round in 0..ROUNDS {
            let node_a = tokio::spawn(node_local_update(format!("node-a-{round}")));
            let node_b = tokio::spawn(node_local_update(format!("node-b-{round}")));
            node_a.await.expect("node a task").expect("node a update");
            node_b.await.expect("node b task").expect("node b update");
        }

        let final_state = load_site_replication_state().await.expect("reload");
        let pending = final_state.pending_rotation.expect("pending rotation survives");
        for round in 0..ROUNDS {
            for node in ["node-a", "node-b"] {
                assert!(
                    pending.secret_candidates.contains(&format!("{node}-{round}")),
                    "{node} update {round} was lost across nodes; candidates: {:?}",
                    pending.secret_candidates
                );
            }
        }
    }

    /// P1-15 review follow-up: the sending side of the peer-edit ordering
    /// fence, driven by two separate nodes. The generation is unique only
    /// because it is allocated inside the state transaction, under the
    /// distributed state-object lock. Two nodes sharing a generation would
    /// leave the receiver unable to tell which edit is newer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_peer_edit_generations_are_unique_across_nodes() {
        publish_ready_iam_context().await;
        // A configured site: `persist_site_replication_state_no_lock` clears
        // the object once a site drops below two peers, and a cleared object
        // would reset the counter between allocations.
        let seed = SiteReplicationState {
            peers: ["site-a", "site-b"]
                .into_iter()
                .map(|name| (name.to_string(), peer(name, &format!("https://{name}.example:9000"))))
                .collect(),
            ..Default::default()
        };
        save_site_replication_state(&seed).await.expect("seed state");

        fn node_local_allocate() -> impl std::future::Future<Output = S3Result<u64>> {
            update_site_replication_state(|state| Ok(next_peer_edit_generation(state)))
        }

        const ROUNDS: usize = 8;
        let mut generations = Vec::new();
        for _ in 0..ROUNDS {
            let node_a = tokio::spawn(node_local_allocate());
            let node_b = tokio::spawn(node_local_allocate());
            generations.push(node_a.await.expect("node a task").expect("node a allocation"));
            generations.push(node_b.await.expect("node b task").expect("node b allocation"));
        }

        let unique: BTreeSet<u64> = generations.iter().copied().collect();
        assert_eq!(
            unique.len(),
            generations.len(),
            "two nodes took the same edit generation, so their deliveries cannot be ordered: {generations:?}"
        );
        assert_eq!(
            load_site_replication_state().await.expect("reload").edit_generation,
            generations.len() as u64,
            "the persisted counter must account for every allocation"
        );
    }

    /// P1-15 (rustfs/backlog#1675 B2): every state RMW — including the
    /// retry-event writers that hang off the hook broadcast paths — now runs
    /// through `update_site_replication_state`, which holds the process
    /// mutex plus the distributed state-object write lock for the whole
    /// load -> mutate -> persist. Before the fix the retry writers took no
    /// lock at all: this concurrent mix deterministically lost one side
    /// (the red-light commit pinned the exact interleaving).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[serial]
    async fn test_retry_event_persist_must_not_wipe_concurrent_locked_rmw() {
        publish_ready_iam_context().await;

        let seed = SiteReplicationState {
            pending_rotation: Some(PendingRotation {
                id: "rot-1".to_string(),
                access_key: "svc-account".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        };
        save_site_replication_state(&seed).await.expect("seed state");

        const ROUNDS: usize = 8;
        for round in 0..ROUNDS {
            let peer = PeerInfo {
                endpoint: format!("https://peer-{round}.example:9000"),
                deployment_id: format!("peer-{round}-deployment"),
                ..Default::default()
            };
            let enqueue = tokio::spawn(async move {
                let error = S3Error::with_message(S3ErrorCode::InternalError, "peer offline".to_string());
                enqueue_site_replication_retry_event(&peer, "bucket-meta", &error).await;
            });
            let ack_id = format!("ack-{round}-deployment");
            let ack = tokio::spawn(async move {
                mark_pending_rotation_peer_acked("rot-1", &ack_id)
                    .await
                    .expect("locked writer must succeed");
            });
            enqueue.await.expect("enqueue task");
            ack.await.expect("ack task");
        }

        let final_state = load_site_replication_state().await.expect("reload final state");
        assert_eq!(
            final_state.retry_queue.len(),
            ROUNDS,
            "every concurrently-enqueued retry event must survive"
        );
        let acked = &final_state
            .pending_rotation
            .as_ref()
            .expect("pending rotation must survive")
            .acked_deployment_ids;
        for round in 0..ROUNDS {
            assert!(
                acked.contains(&format!("ack-{round}-deployment")),
                "rotation ack {round} must survive the concurrent retry-event writers; acked: {acked:?}"
            );
        }
    }
}
