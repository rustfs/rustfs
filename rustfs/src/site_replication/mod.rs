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

//! Site-replication service subsystem (backlog#1840).
//!
//! The parts of site replication that storage-side flows call into — the
//! persisted cluster state and its RMW transaction, the peer HTTP transport,
//! the retry queue, the repair state machine, and the bucket/IAM broadcast
//! hooks — live here in the infra layer. The admin HTTP handlers stay in
//! `crate::admin::handlers::site_replication` and call down into this module;
//! that file re-exports these items so existing paths keep resolving.
//!
//! Storage access goes through the root facade (`crate::storage_api`) and
//! never through the admin or storage interface layers — this module sits
//! below the interface layer and must not import upward.

pub(crate) mod identity;
pub(crate) mod state_lock;

pub(crate) mod hooks;
pub(crate) mod repair;
pub(crate) mod retry;
pub(crate) mod state;
pub(crate) mod transport;

#[cfg(test)]
mod tests;

pub(crate) use self::hooks::*;
pub(crate) use self::repair::*;
pub(crate) use self::retry::*;
pub(crate) use self::state::*;
pub(crate) use self::transport::*;

use self::identity::{
    canonical_endpoint, deployment_id_for_endpoint, mark_unknown_peer_sync_enabled, normalize_peer_map_by_identity_with,
    same_identity_endpoint,
};
use self::state_lock::{SITE_REPLICATION_STATE_PATH, with_site_replication_state_lock};
use crate::auth::constant_time_eq;
use crate::config::get_config_snapshot;
use crate::error::ApiError;
use crate::runtime_sources::{
    current_deployment_id, current_endpoints_handle, current_iam_handle, current_object_store_handle, current_region,
};
use crate::storage_api::site_replication::s3::{
    Body, BucketLifecycleConfiguration, BucketVersioningStatus, DeleteMarkerReplication, DeleteMarkerReplicationStatus,
    DeleteReplication, DeleteReplicationStatus, Destination, ExistingObjectReplication, ExistingObjectReplicationStatus,
    LifecycleRule, ReplicaModifications, ReplicaModificationsStatus, ReplicationConfiguration, ReplicationRule,
    ReplicationRuleStatus, S3Error, S3ErrorCode, S3Response, S3Result, SourceSelectionCriteria, VersioningConfiguration,
    s3_error,
};
#[cfg(test)]
use crate::storage_api::site_replication::save_config as save_admin_config;
use crate::storage_api::site_replication::{
    ARN, BUCKET_REPLICATION_CONFIG, BUCKET_TARGETS_FILE, BUCKET_VERSIONING_CONFIG, BucketOperations, BucketOptions, BucketTarget,
    BucketTargetSys, BucketTargetType, BucketTargets, Credentials, ECStore, OperatorRuleContract, StorageError,
    VersioningApi as _, assign_site_replication_rule_priorities, delete_config_no_lock, deserialize, is_site_replication_role,
    lock_bucket_targets_metadata, metadata_sys, read_config as read_admin_config, read_config_no_lock,
    replication_target_arn_deployment_id, save_config_no_lock, serialize, site_replication_rule_deployment_id,
    with_config_object_read_lock, with_config_object_write_lock,
};
use base64_simd::STANDARD as BASE64_STANDARD;
use base64_simd::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use http::header::{CONTENT_TYPE, HOST};
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use rustfs_config::{DEFAULT_CONSOLE_ADDRESS, DEFAULT_RUSTFS_TLS_PATH, ENV_RUSTFS_CONSOLE_ADDRESS, ENV_RUSTFS_TLS_PATH};
use rustfs_iam::store::{MappedPolicy, UserType, sr_wire_user_type};
use rustfs_iam::sys::SITE_REPLICATOR_SERVICE_ACCOUNT;
use rustfs_madmin::{
    AddOrUpdateUserReq, GroupAddRemove, GroupStatus, PeerInfo, PeerSite, ReplicateEditStatus, SITE_REPL_API_VERSION,
    SRBucketInfo, SRBucketMeta, SRGroupInfo, SRIAMItem, SRIAMPolicy, SRInfo, SRPolicyMapping, SRRemoveReq, SRResyncOpStatus,
    SRRetryStats, SRStateInfo, SyncStatus,
};
use rustfs_signer::constants::UNSIGNED_PAYLOAD;
use rustfs_signer::sign_v4;
use rustfs_tls_runtime::{GlobalPublishedOutboundTlsState, TlsGeneration};
use rustfs_utils::egress::{OutboundUrlError, validate_outbound_url};
use rustfs_utils::http::get_source_scheme;
use rustls_pki_types::pem::PemObject;
use serde::Deserialize;
use serde::Serialize;
use serde::de::IgnoredAny;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::{Mutex, RwLock};
use tracing::{info, warn};
use url::{Url, form_urlencoded};
use uuid::Uuid;

pub(crate) const LOG_COMPONENT_ADMIN: &str = "admin";

pub(crate) const LOG_SUBSYSTEM_SITE_REPLICATION: &str = "site_replication";

pub(crate) const EVENT_ADMIN_SITE_REPLICATION_STATE: &str = "admin_site_replication_state";

/// Layer-local mirror of `crate::admin::utils::json_response` (the repair
/// executor answers the admin HTTP surface but must not import upward from
/// the infra layer).
fn json_response<T: Serialize>(status: StatusCode, value: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(value)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to serialize response: {e}")))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    Ok(S3Response::with_headers((status, Body::from(data)), headers))
}

// The admin layer's runtime-source wrappers apply fallbacks on top of
// `crate::runtime_sources`; this module reproduces the same fallbacks locally
// (verbatim from `crate::admin::runtime_sources`) so it never imports upward
// into the interface layer.

#[cfg(test)]
static TEST_OUTBOUND_TLS_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(test)]
pub(crate) fn set_test_outbound_tls_generation(generation: u64) {
    crate::runtime_sources::set_test_outbound_tls_generation(generation);
    TEST_OUTBOUND_TLS_GENERATION.store(generation, std::sync::atomic::Ordering::Relaxed);
}

fn current_outbound_tls_generation() -> TlsGeneration {
    crate::runtime_sources::current_outbound_tls_generation().unwrap_or_else(empty_outbound_tls_generation)
}

#[cfg(test)]
fn empty_outbound_tls_generation() -> TlsGeneration {
    TlsGeneration(TEST_OUTBOUND_TLS_GENERATION.load(std::sync::atomic::Ordering::Relaxed))
}

#[cfg(not(test))]
fn empty_outbound_tls_generation() -> TlsGeneration {
    TlsGeneration(0)
}

async fn current_outbound_tls_state() -> GlobalPublishedOutboundTlsState {
    if let Some(state) = crate::runtime_sources::current_outbound_tls_state().await {
        return state;
    }

    crate::runtime_sources::fallback_outbound_tls_runtime_interface()
        .state()
        .await
}

fn current_runtime_port() -> u16 {
    crate::runtime_sources::current_runtime_port().unwrap_or(rustfs_config::DEFAULT_PORT)
}
