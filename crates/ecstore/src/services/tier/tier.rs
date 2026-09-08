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
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]

use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use futures::{FutureExt, StreamExt, stream::FuturesUnordered};
use http::HeaderMap;
use http::status::StatusCode;
use lazy_static::lazy_static;
use rand::{Rng, RngExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    io::{self, Cursor},
    ops::Deref,
    panic::AssertUnwindSafe,
    sync::{
        Arc, LazyLock, Mutex, MutexGuard, RwLock as StdRwLock, Weak,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use time::OffsetDateTime;
use tokio::io::BufReader;
use tokio::{
    select,
    sync::{Notify, RwLock},
    time::{Instant, interval, interval_at, timeout_at},
};
use tracing::{debug, error, info, warn};

use crate::error::{Error, Result, StorageError, stable_io_error};
use crate::services::tier::{
    tier_admin::TierCreds,
    tier_config::{TIER_CREDENTIAL_REDACTED, TierConfig, TierType, TierWasabi},
    tier_handlers::{ERR_TIER_ALREADY_EXISTS, ERR_TIER_NAME_NOT_UPPERCASE, ERR_TIER_NOT_FOUND, ERR_TIER_RESERVED_NAME},
    warm_backend::{
        TransitionCandidateProbe, WARM_BACKEND_PROBE_TIMEOUT, WarmBackend, check_warm_backend, check_warm_backend_until,
        new_warm_backend,
    },
};
use crate::storage_api_contracts::{
    bucket::BucketOperations,
    list::{ListOperations, StorageListObjectVersionsInfo, StorageListObjectsV2Info, StorageObjectInfoOrErr, StorageWalkOptions},
    namespace::NamespaceLocking,
    object::{
        DeletedObject, EcstoreObjectIO, EcstoreObjectOperations, HTTPPreconditions, ObjectIO, ObjectOperations, ObjectToDelete,
    },
    range::HTTPRangeSpec,
};
use crate::{
    bucket::lifecycle::{
        get_lifecycle_config,
        tier_delete_journal::{TIER_DELETE_JOURNAL_PREFIX, decode_tier_delete_journal_entry},
        transition_transaction::{TRANSITION_TRANSACTION_RECORD_PREFIX, decode_transition_transaction_record},
    },
    cluster::rpc::peer_rest_client::{PeerRestClient, PeerTierMutationState, tier_mutation_error_is_definitely_rejected},
    config::com::{CONFIG_PREFIX, read_config, read_config_with_metadata},
    disk::{MIGRATING_META_BUCKET, RUSTFS_META_BUCKET},
    layout::endpoints::EndpointServerPools,
    object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader},
    runtime::sources as runtime_sources,
    set_disk::get_lock_acquire_timeout,
    store::ECStore,
};
use rustfs_filemeta::FileInfo;
use rustfs_rio::HashReader;
use rustfs_s3_client::{admin_handler_utils::AdminError, provider_versions::ProviderVersionCapabilities};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join};
use s3s::{S3ErrorCode, dto::BucketLifecycleConfiguration};

use super::{
    tier_handlers::{ERR_TIER_BUCKET_NOT_FOUND, ERR_TIER_CONNECT_ERR, ERR_TIER_INVALID_CREDENTIALS, ERR_TIER_PERM_ERR},
    tier_mutation_intent::{
        TierMutationDigest, TierMutationIntent, TierMutationIntentKind, TierMutationIntentState, TierMutationIntentTarget,
        acquire_tier_mutation_mutex, advance_tier_coordinator_mutation_intent_record_idempotent,
        advance_tier_mutation_intent_record_idempotent, delete_tier_coordinator_mutation_intent_record_if_current,
        delete_tier_mutation_intent_record_if_current, list_tier_coordinator_mutation_intent_records,
        list_tier_mutation_intent_records, load_tier_coordinator_mutation_intent_record_with_etag,
        load_tier_mutation_intent_record_with_etag, save_tier_coordinator_mutation_intent_record_if_absent,
    },
    warm_backend::WarmBackendImpl,
};

const TIER_CFG_REFRESH: Duration = Duration::from_secs(15 * 60);
const TIER_OPERATION_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const TIER_REMOTE_VALIDATION_TIMEOUT: Duration = Duration::from_secs(30);
const TIER_MUTATION_PEER_RPC_TIMEOUT: Duration = Duration::from_secs(30);
const TIER_MUTATION_PEER_FANOUT_TIMEOUT: Duration = Duration::from_secs(30);
const TIER_MUTATION_PEER_FANOUT_CONCURRENCY: usize = 4;
const TIER_REFERENCE_PROOF_LIST_LIMIT: i32 = 1000;
const TIER_REFERENCE_PROOF_PHYSICAL_WALK_CONCURRENCY: usize = 4;
const TIER_MUTATION_INTENT_RECOVERY_SCAN_LIMIT: usize = 1000;
const TIER_MUTATION_INTENT_TTL: Duration = Duration::from_secs(15 * 60);
const TIER_MUTATION_TOMBSTONE_CLOCK_SKEW: Duration = Duration::from_secs(5 * 60);
const TIER_MUTATION_BLOCK_MESSAGE: &str = "Remote tier configuration is being replaced";
const TIER_MUTATION_REFRESH_RETRY_BASE: Duration = Duration::from_secs(1);
const TIER_MUTATION_REFRESH_RETRY_CAP: Duration = Duration::from_secs(30);
const EVENT_TIER_CONFIG_REFRESH: &str = "tier_config_refresh";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_TIER: &str = "tier";

type TierReferenceProofWalkOptions = StorageWalkOptions<fn(&rustfs_filemeta::FileInfo) -> bool>;

fn delayed_tier_refresh_interval(period: Duration) -> tokio::time::Interval {
    interval_at(Instant::now() + period, period)
}

fn tier_mutation_refresh_retry_delay(retry_attempt: u32) -> Duration {
    let multiplier = 1_u32 << retry_attempt.min(5);
    TIER_MUTATION_REFRESH_RETRY_BASE
        .checked_mul(multiplier)
        .unwrap_or(TIER_MUTATION_REFRESH_RETRY_CAP)
        .min(TIER_MUTATION_REFRESH_RETRY_CAP)
}

#[cfg(test)]
struct TierDriverBuildBarrier {
    tier_name: String,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}

#[cfg(test)]
static TIER_DRIVER_BUILD_BARRIER: LazyLock<Mutex<Option<Arc<TierDriverBuildBarrier>>>> = LazyLock::new(|| Mutex::new(None));

#[cfg(test)]
pub(crate) type TierDriverTestFactory =
    Arc<dyn Fn(&TierConfig) -> std::result::Result<WarmBackendImpl, AdminError> + Send + Sync + 'static>;

#[cfg(test)]
tokio::task_local! {
    pub(crate) static TIER_DRIVER_TEST_FACTORY: TierDriverTestFactory;
}

#[cfg(test)]
tokio::task_local! {
    static TIER_REFERENCE_PROOF_TEST_BARRIER: Arc<TierDriverBuildBarrier>;
}

#[cfg(test)]
struct TierDriverBuildBarrierGuard;

#[cfg(test)]
impl Drop for TierDriverBuildBarrierGuard {
    fn drop(&mut self) {
        *lock_unpoisoned(&TIER_DRIVER_BUILD_BARRIER) = None;
    }
}

#[cfg(test)]
fn install_tier_driver_build_barrier(tier_name: &str) -> (Arc<TierDriverBuildBarrier>, TierDriverBuildBarrierGuard) {
    let barrier = Arc::new(TierDriverBuildBarrier {
        tier_name: tier_name.to_string(),
        arrived: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    *lock_unpoisoned(&TIER_DRIVER_BUILD_BARRIER) = Some(barrier.clone());
    (barrier, TierDriverBuildBarrierGuard)
}

#[cfg(test)]
fn tier_reference_proof_test_barrier() -> Arc<TierDriverBuildBarrier> {
    Arc::new(TierDriverBuildBarrier {
        tier_name: String::new(),
        arrived: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    })
}

fn tier_validation_timeout(message: impl Into<String>) -> AdminError {
    let mut err = ERR_TIER_BACKEND_IN_USE.clone();
    err.message = message.into();
    err
}

#[cfg(test)]
async fn wait_for_tier_driver_build_barrier(tier_name: &str) {
    #[cfg(test)]
    let test_barrier = { lock_unpoisoned(&TIER_DRIVER_BUILD_BARRIER).clone() };
    if let Some(barrier) = test_barrier
        && barrier.tier_name == tier_name
    {
        barrier.arrived.notify_one();
        barrier
            .release
            .acquire()
            .await
            .expect("tier driver build test barrier should stay open")
            .forget();
    }
}

async fn construct_warm_backend(tier: &TierConfig) -> std::result::Result<WarmBackendImpl, AdminError> {
    #[cfg(test)]
    if let Ok(result) = TIER_DRIVER_TEST_FACTORY.try_with(|factory| factory(tier)) {
        return result;
    }
    new_warm_backend(tier, false).await
}

async fn build_warm_backend(tier: &TierConfig, probe: bool) -> std::result::Result<WarmBackendImpl, AdminError> {
    build_warm_backend_with_deadline(tier, probe, None).await
}

async fn build_warm_backend_with_deadline(
    tier: &TierConfig,
    probe: bool,
    deadline: Option<Instant>,
) -> std::result::Result<WarmBackendImpl, AdminError> {
    #[cfg(test)]
    {
        let wait = wait_for_tier_driver_build_barrier(&tier.name);
        if let Some(deadline) = deadline {
            timeout_at(deadline, wait)
                .await
                .map_err(|_| tier_validation_timeout("Timed out preparing the remote tier backend"))?;
        } else {
            wait.await;
        }
    }

    let driver = if let Some(deadline) = deadline {
        timeout_at(deadline, construct_warm_backend(tier))
            .await
            .map_err(|_| tier_validation_timeout("Timed out preparing the remote tier backend"))??
    } else {
        construct_warm_backend(tier).await?
    };
    if probe {
        if let Some(deadline) = deadline {
            check_warm_backend_until(Some(&driver), deadline).await?;
        } else {
            check_warm_backend(Some(&driver)).await?;
        }
    }
    Ok(driver)
}

const TIER_CONFIG_LEGACY_FILE: &str = "tier-config.json";
pub const TIER_CONFIG_FILE: &str = "tier-config.bin";
pub const TIER_CONFIG_FORMAT: u16 = 1;
pub const TIER_CONFIG_V1: u16 = 1;
pub const TIER_CONFIG_VERSION: u16 = 2;

const EXTERNAL_TIER_TYPE_UNSUPPORTED: i32 = 0;
const EXTERNAL_TIER_TYPE_S3: i32 = 1;
const EXTERNAL_TIER_TYPE_AZURE: i32 = 2;
const EXTERNAL_TIER_TYPE_GCS: i32 = 3;
const EXTERNAL_TIER_TYPE_MINIO: i32 = 4;
const EXTERNAL_TIER_VERSION_WASABI_V1: &str = "rustfs-wasabi-v1";
const EXTERNAL_TIER_WASABI_ENDPOINT_SENTINEL: &str = "rustfs-wasabi-v1:";
const TIER_BACKEND_IDENTITY_VERSION: u8 = 2;
const _TIER_CFG_REFRESH_AT_HDR: &str = "X-RustFS-TierCfg-RefreshedAt";

lazy_static! {
    pub static ref ERR_TIER_MISSING_CREDENTIALS: AdminError = AdminError {
        code: "XRustFSAdminTierMissingCredentials".to_string(),
        message: "Specified remote credentials are empty".to_string(),
        status_code: StatusCode::FORBIDDEN,
    };
    pub static ref ERR_TIER_BACKEND_IN_USE: AdminError = AdminError {
        code: "XRustFSAdminTierBackendInUse".to_string(),
        message: "Specified remote tier is already in use".to_string(),
        status_code: StatusCode::CONFLICT,
    };
    pub static ref ERR_TIER_TYPE_UNSUPPORTED: AdminError = AdminError {
        code: "XRustFSAdminTierTypeUnsupported".to_string(),
        message: "Specified tier type is unsupported".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    };
    pub static ref ERR_TIER_BACKEND_NOT_EMPTY: AdminError = AdminError {
        code: "XRustFSAdminTierBackendNotEmpty".to_string(),
        message: "Specified remote backend is not empty".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    };
    pub static ref ERR_TIER_INVALID_CONFIG: AdminError = AdminError {
        code: "XRustFSAdminTierInvalidConfig".to_string(),
        message: "Unable to setup remote tier, check tier configuration".to_string(),
        status_code: StatusCode::BAD_REQUEST,
    };
}

fn tier_invalid_config(message: impl Into<String>) -> AdminError {
    let mut err = ERR_TIER_INVALID_CONFIG.clone();
    err.message = message.into();
    err
}

fn normalize_add_tier_name_fields(
    canonical_name: &mut String,
    nested_name: &mut String,
    provider: &str,
) -> std::result::Result<(), AdminError> {
    if !canonical_name.is_empty() && !nested_name.is_empty() && canonical_name.as_str() != nested_name.as_str() {
        return Err(tier_invalid_config(format!(
            "TierConfig.Name conflicts with the legacy {provider}.name field"
        )));
    }
    let resolved_name = if canonical_name.is_empty() {
        nested_name.clone()
    } else {
        canonical_name.clone()
    };
    if resolved_name.is_empty() {
        return Err(tier_invalid_config("Remote tier name is empty"));
    }
    canonical_name.clone_from(&resolved_name);
    nested_name.clone_from(&resolved_name);
    Ok(())
}

fn normalize_s3_gcs_add_tier_name(config: &mut TierConfig) -> std::result::Result<(), AdminError> {
    match config.tier_type {
        TierType::S3 => {
            if let Some(s3) = config.s3.as_mut() {
                normalize_add_tier_name_fields(&mut config.name, &mut s3.name, "S3")?;
            }
        }
        TierType::GCS => {
            if let Some(gcs) = config.gcs.as_mut() {
                normalize_add_tier_name_fields(&mut config.name, &mut gcs.name, "GCS")?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn credential_is_redacted(value: &str) -> bool {
    value.trim() == TIER_CREDENTIAL_REDACTED
}

fn validate_static_tier_credentials(access_key: &str, secret_key: &str) -> std::result::Result<(), AdminError> {
    if access_key.is_empty() || secret_key.is_empty() || credential_is_redacted(access_key) || credential_is_redacted(secret_key)
    {
        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
    }
    Ok(())
}

fn tier_creds_request_aws_role(creds: &TierCreds) -> bool {
    creds.aws_role || !creds.aws_role_web_identity_token_file.is_empty() || !creds.aws_role_arn.is_empty()
}

fn s3_config_requests_aws_role(config: &crate::services::tier::tier_config::TierS3) -> bool {
    config.aws_role
        || !config.aws_role_web_identity_token_file.is_empty()
        || !config.aws_role_arn.is_empty()
        || !config.aws_role_session_name.is_empty()
        || config.aws_role_duration_seconds != 0
}

fn reject_unsupported_aws_role() -> AdminError {
    tier_invalid_config("AWS role and web identity credentials are not supported for remote tiers")
}

fn validate_gcs_credentials_json(credentials: &str) -> std::result::Result<(), AdminError> {
    if credentials.is_empty() || credential_is_redacted(credentials) {
        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
    }
    let value: serde_json::Value = serde_json::from_str(credentials)
        .map_err(|_| tier_invalid_config("GCS credentials must be valid service account JSON"))?;
    if value.get("type").and_then(serde_json::Value::as_str) != Some("service_account") {
        return Err(tier_invalid_config("GCS credentials must describe a service account"));
    }
    Ok(())
}

fn validate_azure_supported_options(
    azure: &crate::services::tier::tier_config::TierAzure,
) -> std::result::Result<(), AdminError> {
    let sp_auth_set =
        !azure.sp_auth.tenant_id.is_empty() || !azure.sp_auth.client_id.is_empty() || !azure.sp_auth.client_secret.is_empty();
    if !azure.storage_class.is_empty() || sp_auth_set {
        return Err(tier_invalid_config(
            "Azure remote tiers do not support storageClass or spAuth yet; leave both unset",
        ));
    }
    Ok(())
}

fn validate_tier_config_credentials(config: &TierConfig) -> std::result::Result<(), AdminError> {
    match config.tier_type {
        TierType::S3 => {
            if let Some(s3) = config.s3.as_ref() {
                if s3_config_requests_aws_role(s3) {
                    return Err(reject_unsupported_aws_role());
                }
                validate_static_tier_credentials(&s3.access_key, &s3.secret_key)?;
            }
        }
        TierType::Wasabi => {
            if let Some(backend) = config.wasabi.as_ref() {
                validate_static_tier_credentials(&backend.access_key, &backend.secret_key)?;
            }
        }
        TierType::RustFS => {
            if let Some(backend) = config.rustfs.as_ref() {
                validate_static_tier_credentials(&backend.access_key, &backend.secret_key)?;
            }
        }
        TierType::MinIO => {
            if let Some(backend) = config.minio.as_ref() {
                validate_static_tier_credentials(&backend.access_key, &backend.secret_key)?;
            }
        }
        TierType::Aliyun => {
            if let Some(backend) = config.aliyun.as_ref() {
                validate_static_tier_credentials(&backend.access_key, &backend.secret_key)?;
            }
        }
        TierType::Tencent => {
            if let Some(backend) = config.tencent.as_ref() {
                validate_static_tier_credentials(&backend.access_key, &backend.secret_key)?;
            }
        }
        TierType::Huaweicloud => {
            if let Some(backend) = config.huaweicloud.as_ref() {
                validate_static_tier_credentials(&backend.access_key, &backend.secret_key)?;
            }
        }
        TierType::Azure => {
            if let Some(backend) = config.azure.as_ref() {
                validate_azure_supported_options(backend)?;
                validate_static_tier_credentials(&backend.access_key, &backend.secret_key)?;
            }
        }
        TierType::GCS => {
            if let Some(gcs) = config.gcs.as_ref() {
                validate_gcs_credentials_json(&gcs.creds)?;
            }
        }
        TierType::R2 => {
            if let Some(backend) = config.r2.as_ref() {
                validate_static_tier_credentials(&backend.access_key, &backend.secret_key)?;
            }
        }
        TierType::Unsupported => {}
    }
    Ok(())
}

fn merge_static_tier_credentials(
    access_key: &mut String,
    secret_key: &mut String,
    creds: &TierCreds,
) -> std::result::Result<(), AdminError> {
    if tier_creds_request_aws_role(creds) {
        return Err(reject_unsupported_aws_role());
    }
    if !creds.creds_json.is_empty() {
        return Err(tier_invalid_config("GCS credentials cannot be used with this remote tier type"));
    }
    match (creds.access_key.is_empty(), creds.secret_key.is_empty()) {
        (true, true) => {}
        (false, false) => {
            validate_static_tier_credentials(&creds.access_key, &creds.secret_key)?;
            access_key.clone_from(&creds.access_key);
            secret_key.clone_from(&creds.secret_key);
        }
        _ => return Err(ERR_TIER_MISSING_CREDENTIALS.clone()),
    }
    validate_static_tier_credentials(access_key, secret_key)
}

#[derive(Serialize, Deserialize)]
pub struct TierConfigMgr {
    #[serde(skip)]
    pub driver_cache: HashMap<String, WarmBackendImpl>,
    pub tiers: HashMap<String, TierConfig>,
    pub last_refreshed_at: OffsetDateTime,
}

type SharedWarmBackend = Arc<dyn WarmBackend + Send + Sync + 'static>;

#[derive(Default)]
struct TierDriverRuntime {
    generations: HashMap<String, Arc<TierDriverGeneration>>,
    draining: HashMap<String, u64>,
    prepared_mutation_blocks: HashMap<String, uuid::Uuid>,
    committed_mutation_blocks: HashMap<String, HashSet<uuid::Uuid>>,
    mutation_blocks_revision: u64,
    next_generation: u64,
    next_drain_epoch: u64,
    admin_updates: Arc<tokio::sync::Mutex<()>>,
    mutation_refresh: Arc<Notify>,
}

struct TierDriverRegistryEntry {
    manager: Weak<RwLock<TierConfigMgr>>,
    runtime: Arc<Mutex<TierDriverRuntime>>,
}

struct TierPublishTransition {
    runtime: Arc<Mutex<TierDriverRuntime>>,
    tokens: Vec<(String, u64)>,
    revoked: HashMap<String, Arc<TierDriverGeneration>>,
    replaced_destinations: HashMap<String, TierConfig>,
    mutation_block_allowance: Option<MutationBlockAllowance>,
    published: bool,
}

#[derive(Clone)]
struct MutationBlockAllowance {
    mutation_ids: HashSet<uuid::Uuid>,
    revision: u64,
}

#[derive(Debug, Clone)]
struct RecoveredTierMutationIntent {
    intent: TierMutationIntent,
    has_peer_record: bool,
    has_coordinator_record: bool,
    peer_state: Option<TierMutationIntentState>,
    coordinator_state: Option<TierMutationIntentState>,
}

impl RecoveredTierMutationIntent {
    fn is_peer_only_terminal(&self) -> bool {
        self.has_peer_record && !self.has_coordinator_record && self.intent.state != TierMutationIntentState::Prepared
    }
}

struct TierMutationRecoverySnapshot {
    coordinator_intents: Vec<TierMutationIntent>,
    intents: Vec<RecoveredTierMutationIntent>,
    allowance: MutationBlockAllowance,
    has_committed_mutation_blocks: bool,
}

#[derive(Debug)]
struct CommittedMutationRecoveryPlan {
    replay_order: Vec<uuid::Uuid>,
    coordinator_cleanup_order: Vec<uuid::Uuid>,
    peer_cleanup_order: Vec<uuid::Uuid>,
}

impl TierMutationRecoverySnapshot {
    fn requires_config_lock(&self) -> bool {
        self.has_committed_mutation_blocks || self.intents.iter().any(|recovered| recovered.has_coordinator_record)
    }

    fn allowance(&self) -> MutationBlockAllowance {
        self.allowance.clone()
    }

    fn committed_intents(&self) -> impl Iterator<Item = &RecoveredTierMutationIntent> {
        self.intents.iter().filter(|recovered| {
            recovered.intent.state == TierMutationIntentState::Committed
                && (!recovered.is_peer_only_terminal() || self.allowance.mutation_ids.contains(&recovered.intent.mutation_id))
        })
    }

    fn is_quiescent(&self) -> bool {
        self.allowance.mutation_ids.is_empty() && self.intents.iter().all(RecoveredTierMutationIntent::is_peer_only_terminal)
    }
}

struct PreparedTierDriver {
    tier_name: String,
    tier_config: TierConfig,
    config_fingerprint: TierDriverFingerprint,
    backend_identity: TierDestinationId,
    exact_get_delete: bool,
    driver: SharedWarmBackend,
}

impl TierPublishTransition {
    async fn wait_for_active_leases(&self) -> std::result::Result<(), AdminError> {
        self.wait_for_active_leases_until(Instant::now() + TIER_OPERATION_DRAIN_TIMEOUT)
            .await
    }

    async fn wait_for_active_leases_until(&self, deadline: Instant) -> std::result::Result<(), AdminError> {
        for generation in self.revoked.values() {
            if timeout_at(deadline, generation.wait_for_no_active_leases()).await.is_err() {
                let mut err = ERR_TIER_BACKEND_IN_USE.clone();
                err.message = "Timed out waiting for active remote tier operations to finish".to_string();
                return Err(err);
            }
        }
        Ok(())
    }

    async fn ensure_replaced_destinations_are_empty(&self) -> std::result::Result<(), AdminError> {
        ensure_replaced_destinations_are_empty(&self.replaced_destinations, &self.revoked).await
    }
}

async fn ensure_replaced_destinations_are_empty(
    replaced_destinations: &HashMap<String, TierConfig>,
    revoked: &HashMap<String, Arc<TierDriverGeneration>>,
) -> std::result::Result<(), AdminError> {
    let deadline = Instant::now() + TIER_REMOTE_VALIDATION_TIMEOUT;
    for (tier_name, old_config) in replaced_destinations {
        let prepared;
        let driver = match revoked.get(tier_name) {
            Some(generation) => generation.driver.as_ref(),
            None => {
                prepared = match timeout_at(deadline, build_warm_backend(old_config, false)).await {
                    Ok(result) => result?,
                    Err(_) => {
                        let mut err = ERR_TIER_BACKEND_IN_USE.clone();
                        err.message = "Timed out preparing the replaced remote tier backend".to_string();
                        return Err(err);
                    }
                };
                prepared.as_ref()
            }
        };
        match timeout_at(deadline, driver.in_use()).await {
            Ok(Ok(false)) => {}
            Ok(Ok(true)) => return Err(ERR_TIER_BACKEND_NOT_EMPTY.clone()),
            Ok(Err(err)) => {
                let mut admin_err = ERR_TIER_PERM_ERR.clone();
                admin_err.message.push('.');
                admin_err.message.push_str(&err.to_string());
                return Err(admin_err);
            }
            Err(_) => {
                let mut err = ERR_TIER_BACKEND_IN_USE.clone();
                err.message = "Timed out checking whether the replaced remote tier backend is empty".to_string();
                return Err(err);
            }
        }
    }
    Ok(())
}

impl Drop for TierPublishTransition {
    fn drop(&mut self) {
        let mut runtime = lock_unpoisoned(&self.runtime);
        for (tier_name, epoch) in &self.tokens {
            if runtime.draining.get(tier_name) == Some(epoch) {
                if !self.published
                    && !runtime.generations.contains_key(tier_name)
                    && let Some(generation) = self.revoked.get(tier_name)
                {
                    generation.accepting.store(true, Ordering::Release);
                    runtime.generations.insert(tier_name.clone(), generation.clone());
                }
                runtime.draining.remove(tier_name);
            }
        }
    }
}

static TIER_DRIVER_REGISTRY: LazyLock<StdRwLock<HashMap<usize, TierDriverRegistryEntry>>> =
    LazyLock::new(|| StdRwLock::new(HashMap::new()));

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn tier_manager_key(manager: &TierConfigMgr) -> usize {
    std::ptr::from_ref(manager).addr()
}

fn tier_driver_runtime(handle: &Arc<RwLock<TierConfigMgr>>, manager: &TierConfigMgr) -> Arc<Mutex<TierDriverRuntime>> {
    let key = tier_manager_key(manager);
    {
        let registry = TIER_DRIVER_REGISTRY.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(entry) = registry.get(&key)
            && entry
                .manager
                .upgrade()
                .is_some_and(|registered| Arc::ptr_eq(&registered, handle))
        {
            return entry.runtime.clone();
        }
    }
    #[cfg(test)]
    let _ = TIER_REGISTRY_MISS_SCAN_COUNT.try_with(|count| count.set(count.get() + 1));
    let mut registry = TIER_DRIVER_REGISTRY
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if let Some(entry) = registry.get(&key)
        && entry
            .manager
            .upgrade()
            .is_some_and(|registered| Arc::ptr_eq(&registered, handle))
    {
        return entry.runtime.clone();
    }
    registry.retain(|_, entry| entry.manager.strong_count() > 0);
    let runtime = Arc::new(Mutex::new(TierDriverRuntime::default()));
    registry.insert(
        key,
        TierDriverRegistryEntry {
            manager: Arc::downgrade(handle),
            runtime: runtime.clone(),
        },
    );
    runtime
}

#[cfg(test)]
tokio::task_local! {
    static TIER_REGISTRY_MISS_SCAN_COUNT: std::cell::Cell<usize>;
}

fn registered_tier_driver_runtime(manager: &TierConfigMgr) -> Option<Arc<Mutex<TierDriverRuntime>>> {
    let key = tier_manager_key(manager);
    {
        let registry = TIER_DRIVER_REGISTRY.read().unwrap_or_else(std::sync::PoisonError::into_inner);
        let entry = registry.get(&key)?;
        if entry.manager.strong_count() > 0 {
            return Some(entry.runtime.clone());
        }
    }
    let mut registry = TIER_DRIVER_REGISTRY
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if registry.get(&key).is_some_and(|entry| entry.manager.strong_count() == 0) {
        registry.remove(&key);
    }
    None
}

fn runtime_blocks_tier(runtime: &TierDriverRuntime, tier_name: &str) -> bool {
    runtime.draining.contains_key(tier_name)
        || runtime.prepared_mutation_blocks.contains_key(tier_name)
        || runtime.committed_mutation_blocks.contains_key(tier_name)
}

fn runtime_mutation_blocks_tier_transition(
    runtime: &TierDriverRuntime,
    tier_name: &str,
    allowance: Option<&MutationBlockAllowance>,
) -> bool {
    if runtime
        .prepared_mutation_blocks
        .get(tier_name)
        .is_some_and(|mutation_id| !allowance.is_some_and(|allowance| allowance.mutation_ids.contains(mutation_id)))
    {
        return true;
    }
    runtime
        .committed_mutation_blocks
        .get(tier_name)
        .is_some_and(|mutation_ids| !allowance.is_some_and(|allowance| mutation_ids.is_subset(&allowance.mutation_ids)))
}

fn runtime_blocks_tier_transition(
    runtime: &TierDriverRuntime,
    tier_name: &str,
    allowance: Option<&MutationBlockAllowance>,
) -> bool {
    runtime.draining.contains_key(tier_name) || runtime_mutation_blocks_tier_transition(runtime, tier_name, allowance)
}

fn runtime_has_mutation_block(runtime: &TierDriverRuntime) -> bool {
    !runtime.draining.is_empty() || !runtime.prepared_mutation_blocks.is_empty() || !runtime.committed_mutation_blocks.is_empty()
}

type TierDriverFingerprint = [u8; 32];
pub(crate) type TierDestinationId = [u8; 32];
pub(crate) type DriverRevision = u64;

fn tier_config_fingerprint(tier_name: &str, config: &TierConfig) -> io::Result<TierDriverFingerprint> {
    let external = to_external_tier_config(tier_name, config)?;
    let encoded = rmp_serde::to_vec_named(&external)
        .map_err(|err| io::Error::other(format!("serialize tier driver identity failed: {err}")))?;
    Ok(Sha256::digest(encoded).into())
}

fn tier_configs_match(tier_name: &str, current: &TierConfig, replacement: &TierConfig) -> bool {
    match (
        tier_config_fingerprint(tier_name, current),
        tier_config_fingerprint(tier_name, replacement),
    ) {
        (Ok(current), Ok(replacement)) => current == replacement,
        _ => false,
    }
}

#[derive(Debug)]
pub enum TierConfigUpdateError {
    Load(io::Error),
    Mutation(AdminError),
    Save(io::Error),
    Publish(AdminError),
}

enum TierCandidateMutation {
    Add(Box<TierConfig>, bool),
    Edit(String, TierCreds),
    Remove(String, bool),
    Clear(bool),
    Prevalidated(Box<PrevalidatedTierCandidateMutation>),
}

struct PrevalidatedTierCandidateMutation {
    candidate: TierConfigMgr,
    current: TierConfigMgr,
    version: Option<String>,
    kind: TierMutationIntentKind,
    explicit_tier_name: Option<String>,
    force: bool,
    driver_tier: Option<String>,
    candidate_digest: TierMutationDigest,
}

impl TierCandidateMutation {
    fn add(mut config: TierConfig, force: bool) -> std::result::Result<Self, AdminError> {
        normalize_s3_gcs_add_tier_name(&mut config)?;
        Ok(Self::Add(Box::new(config), force))
    }

    fn normalize_add_tier_name(&mut self) -> std::result::Result<(), AdminError> {
        if let Self::Add(config, _) = self {
            normalize_s3_gcs_add_tier_name(config)?;
        }
        Ok(())
    }

    fn intent_kind(&self) -> TierMutationIntentKind {
        match self {
            Self::Add(_, _) => TierMutationIntentKind::Add,
            Self::Edit(_, _) => TierMutationIntentKind::Edit,
            Self::Remove(_, _) => TierMutationIntentKind::Remove,
            Self::Clear(_) => TierMutationIntentKind::Clear,
            Self::Prevalidated(prepared) => prepared.kind,
        }
    }

    /// `force` as supplied by the caller, or `false` for `Edit` (credential rebind never
    /// accepts a force override). Used to gate the lifecycle-config reference check, which
    /// unlike the persisted/physical-object reference checks is meant to be force-bypassable.
    fn force(&self) -> bool {
        match self {
            Self::Add(_, force) | Self::Remove(_, force) | Self::Clear(force) => *force,
            Self::Edit(_, _) => false,
            Self::Prevalidated(prepared) => prepared.force,
        }
    }

    fn explicit_tier_name(&self) -> Option<&str> {
        match self {
            Self::Add(config, _) => Some(&config.name),
            Self::Edit(tier_name, _) | Self::Remove(tier_name, _) => Some(tier_name),
            Self::Clear(_) => None,
            Self::Prevalidated(prepared) => prepared.explicit_tier_name.as_deref(),
        }
    }

    fn target_tiers(&self, manager: &TierConfigMgr, candidate: &TierConfigMgr) -> HashSet<String> {
        let mut targets = changed_tier_names(manager, candidate);
        match self {
            Self::Add(config, _) => {
                targets.insert(config.name.clone());
            }
            Self::Edit(tier_name, _) => {
                targets.insert(tier_name.clone());
            }
            Self::Remove(tier_name, _) => {
                if manager.tiers.contains_key(tier_name) || candidate.tiers.contains_key(tier_name) {
                    targets.insert(tier_name.clone());
                }
            }
            Self::Clear(_) => {
                targets.extend(manager.tiers.keys().chain(candidate.tiers.keys()).cloned());
            }
            Self::Prevalidated(_) => {
                targets.extend(changed_tier_names(manager, candidate));
            }
        }
        targets
    }

    #[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
    fn affected_targets(
        &self,
        manager: &TierConfigMgr,
        candidate: &TierConfigMgr,
    ) -> std::result::Result<Vec<TierMutationIntentTarget>, AdminError> {
        let explicit_tier_name = self.explicit_tier_name();
        build_tier_mutation_affected_targets(
            self.intent_kind(),
            tier_mutation_proof_targets(self.intent_kind(), explicit_tier_name, manager, candidate),
            manager,
            candidate,
        )
    }

    async fn apply(
        self,
        candidate: &mut TierConfigMgr,
        deadline: Option<Instant>,
    ) -> std::result::Result<Option<String>, AdminError> {
        match self {
            Self::Add(config, force) => {
                let tier_name = config.name.clone();
                candidate.add_with_deadline(*config, force, deadline).await?;
                Ok(Some(tier_name))
            }
            Self::Edit(tier_name, credentials) => {
                candidate.edit_with_deadline(&tier_name, credentials, deadline).await?;
                Ok(Some(tier_name))
            }
            Self::Remove(tier_name, force) => {
                candidate.remove(&tier_name, force).await?;
                Ok(None)
            }
            Self::Clear(force) => {
                candidate.clear_tier(force).await?;
                Ok(None)
            }
            Self::Prevalidated(_) => unreachable!("prevalidated tier mutation must bypass backend validation"),
        }
    }
}

fn tier_mutation_proof_targets(
    kind: TierMutationIntentKind,
    explicit_tier_name: Option<&str>,
    current: &TierConfigMgr,
    candidate: &TierConfigMgr,
) -> HashSet<String> {
    let mut targets = HashSet::new();
    match kind {
        TierMutationIntentKind::Add | TierMutationIntentKind::Edit | TierMutationIntentKind::Remove => {
            if let Some(tier_name) = explicit_tier_name
                && (current.tiers.contains_key(tier_name) || candidate.tiers.contains_key(tier_name))
            {
                targets.insert(tier_name.to_string());
            }
        }
        TierMutationIntentKind::Clear => {
            targets.extend(current.tiers.keys().chain(candidate.tiers.keys()).cloned());
        }
    }
    targets
}

fn build_tier_mutation_affected_targets(
    kind: TierMutationIntentKind,
    target_tiers: HashSet<String>,
    current: &TierConfigMgr,
    candidate: &TierConfigMgr,
) -> std::result::Result<Vec<TierMutationIntentTarget>, AdminError> {
    let mut targets = target_tiers.into_iter().collect::<Vec<_>>();
    targets.sort();
    let mut out = Vec::with_capacity(targets.len());
    for tier_name in targets {
        let old_backend_identity = current
            .tiers
            .get(&tier_name)
            .map(tier_backend_identity)
            .transpose()
            .map_err(tier_backend_identity_admin_error)?;
        let new_backend_identity = candidate
            .tiers
            .get(&tier_name)
            .map(tier_backend_identity)
            .transpose()
            .map_err(tier_backend_identity_admin_error)?;
        if old_backend_identity.is_none() && new_backend_identity.is_none() {
            continue;
        }
        let target = TierMutationIntentTarget {
            tier_name,
            old_backend_identity,
            new_backend_identity,
        };
        validate_tier_mutation_target_shape(kind, &target)?;
        out.push(target);
    }
    Ok(out)
}

fn validate_tier_mutation_target_shape(
    kind: TierMutationIntentKind,
    target: &TierMutationIntentTarget,
) -> std::result::Result<(), AdminError> {
    let valid = match kind {
        TierMutationIntentKind::Add => target.old_backend_identity.is_none() && target.new_backend_identity.is_some(),
        TierMutationIntentKind::Edit => target.old_backend_identity.is_some() && target.new_backend_identity.is_some(),
        TierMutationIntentKind::Remove | TierMutationIntentKind::Clear => {
            target.old_backend_identity.is_some() && target.new_backend_identity.is_none()
        }
    };
    if valid {
        return Ok(());
    }
    let mut err = ERR_TIER_INVALID_CONFIG.clone();
    err.message = "Remote tier mutation target identity proof does not match mutation kind".to_string();
    Err(err)
}

fn tier_backend_identity_admin_error(err: io::Error) -> AdminError {
    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
    admin_err.message = err.to_string();
    admin_err
}

#[async_trait::async_trait]
trait TierReferenceProofStore:
    EcstoreObjectIO
    + NamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>
    + BucketOperations<Error = Error>
    + ListOperations<
        Error = Error,
        ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>,
        ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>,
        ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>,
        WalkOptions = TierReferenceProofWalkOptions,
        WalkCancellation = tokio_util::sync::CancellationToken,
        WalkResultSender = tokio::sync::mpsc::Sender<StorageObjectInfoOrErr<ObjectInfo, Error>>,
    > + Send
    + Sync
{
    async fn authoritative_buckets_for_reference_proof(&self) -> Result<Vec<crate::storage_api_contracts::bucket::BucketInfo>> {
        self.list_bucket(&crate::storage_api_contracts::bucket::BucketOptions {
            deleted: true,
            no_metadata: true,
            ..Default::default()
        })
        .await
    }

    async fn lifecycle_config_for_reference_proof(&self, bucket: &str) -> Result<Option<BucketLifecycleConfiguration>>;

    async fn authoritative_bucket_tier_reference(
        self: Arc<Self>,
        bucket: &str,
        targets: &[TierMutationIntentTarget],
    ) -> std::result::Result<Option<ObjectInfo>, AdminError>
    where
        Self: Sized,
    {
        find_authoritative_bucket_tier_reference_by_walk(self, bucket, targets).await
    }
}

#[async_trait::async_trait]
impl TierReferenceProofStore for ECStore {
    async fn authoritative_buckets_for_reference_proof(&self) -> Result<Vec<crate::storage_api_contracts::bucket::BucketInfo>> {
        let opts = crate::storage_api_contracts::bucket::BucketOptions {
            deleted: true,
            no_metadata: true,
            ..Default::default()
        };
        let (logical, physical) = tokio::join!(
            <ECStore as BucketOperations>::list_bucket(self, &opts),
            self.list_bucket_for_scanner(&opts),
        );
        let logical = logical?;
        let physical = physical?;
        if !physical.topology_complete {
            return Err(Error::other(
                "tier reference proof cannot enumerate every physical bucket with complete set quorum",
            ));
        }
        let mut buckets = BTreeMap::new();
        for bucket in logical.into_iter().chain(physical.buckets) {
            buckets.entry(bucket.name.clone()).or_insert(bucket);
        }
        Ok(buckets.into_values().collect())
    }

    async fn lifecycle_config_for_reference_proof(&self, bucket: &str) -> Result<Option<BucketLifecycleConfiguration>> {
        match get_lifecycle_config(bucket).await {
            Ok((config, _updated_at)) => Ok(Some(config)),
            Err(Error::ConfigNotFound) => Ok(None),
            Err(err) => Err(err),
        }
    }

    async fn authoritative_bucket_tier_reference(
        self: Arc<Self>,
        bucket: &str,
        targets: &[TierMutationIntentTarget],
    ) -> std::result::Result<Option<ObjectInfo>, AdminError> {
        find_authoritative_bucket_tier_reference_on_physical_sets(self, bucket, targets).await
    }
}

async fn ensure_no_authoritative_tier_object_references<S>(
    api: Arc<S>,
    affected_targets: &[TierMutationIntentTarget],
    force: bool,
) -> std::result::Result<(), AdminError>
where
    S: TierReferenceProofStore,
{
    let targets = affected_targets
        .iter()
        .filter(|target| target.old_backend_identity.is_some() && target.old_backend_identity != target.new_backend_identity)
        .cloned()
        .collect::<Vec<_>>();
    if targets.is_empty() {
        return Ok(());
    }
    #[cfg(test)]
    let test_barrier = TIER_REFERENCE_PROOF_TEST_BARRIER.try_with(Arc::clone).ok();
    #[cfg(test)]
    if let Some(barrier) = test_barrier {
        barrier.arrived.notify_one();
        barrier
            .release
            .acquire()
            .await
            .expect("tier reference proof test semaphore should stay open")
            .forget();
    }
    ensure_no_authoritative_target_references(api, &targets, force).await
}

async fn ensure_no_authoritative_target_references<S>(
    api: Arc<S>,
    targets: &[TierMutationIntentTarget],
    force: bool,
) -> std::result::Result<(), AdminError>
where
    S: TierReferenceProofStore,
{
    let buckets = api
        .authoritative_buckets_for_reference_proof()
        .await
        .map_err(tier_reference_proof_admin_error)?;
    for bucket in buckets {
        // The lifecycle-config check only warns about a *future* transition attempt against
        // this tier name, not an existing object/journal/transaction reference — unlike those,
        // it is meant to be bypassable with `force`, mirroring `TierConfigMgr::remove()`'s
        // `!force` gate on its own `driver.in_use()` probe (rustfs/backlog#2077).
        if !force {
            ensure_no_authoritative_lifecycle_references(api.as_ref(), &bucket.name, targets).await?;
        }
        if let Some(object) = api.clone().authoritative_bucket_tier_reference(&bucket.name, targets).await? {
            return Err(tier_reference_proof_in_use_error(&object.transitioned_object.tier, &object));
        }
    }
    ensure_no_authoritative_persisted_references(api, targets).await
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) async fn ensure_no_authoritative_tier_references_for_test(
    api: Arc<ECStore>,
    tier_name: &str,
    old_backend_identity: TierDestinationId,
    force: bool,
) -> std::result::Result<(), AdminError> {
    ensure_no_authoritative_tier_object_references(
        api,
        &[TierMutationIntentTarget {
            tier_name: tier_name.to_string(),
            old_backend_identity: Some(old_backend_identity),
            new_backend_identity: None,
        }],
        force,
    )
    .await
}

async fn find_authoritative_bucket_tier_reference_by_walk<S>(
    api: Arc<S>,
    bucket: &str,
    targets: &[TierMutationIntentTarget],
) -> std::result::Result<Option<ObjectInfo>, AdminError>
where
    S: TierReferenceProofStore,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel::<StorageObjectInfoOrErr<ObjectInfo, Error>>(100);
    let cancellation = tokio_util::sync::CancellationToken::new();
    let walk = api.clone().walk(
        cancellation.clone(),
        bucket,
        "",
        tx,
        TierReferenceProofWalkOptions {
            include_free_versions: true,
            // Namespace size determines total proof time. Drive-level progress
            // stalls remain bounded by the walk implementation.
            walkdir_timeout: Some(Duration::ZERO),
            ..Default::default()
        },
    );
    let collect = async {
        while let Some(result) = rx.recv().await {
            if let Some(err) = result.err {
                cancellation.cancel();
                return Err(err);
            }
            let Some(object) = result.item else {
                continue;
            };
            match tier_object_blocks_any_target_rebind(&object, targets) {
                Ok(true) => {
                    cancellation.cancel();
                    return Ok(Some(object));
                }
                Ok(false) => {}
                Err(err) => {
                    cancellation.cancel();
                    return Err(Error::other(err));
                }
            }
        }
        Ok(None)
    };
    let (walk_result, collect_result) = tokio::join!(walk, collect);
    match collect_result.map_err(tier_reference_proof_admin_error)? {
        Some(blocker) => Ok(Some(blocker)),
        None => {
            walk_result.map_err(tier_reference_proof_admin_error)?;
            Ok(None)
        }
    }
}

async fn find_authoritative_bucket_tier_reference_on_physical_sets(
    api: Arc<ECStore>,
    bucket: &str,
    targets: &[TierMutationIntentTarget],
) -> std::result::Result<Option<ObjectInfo>, AdminError> {
    use futures::StreamExt as _;

    let physical_sets = api
        .pools
        .iter()
        .flat_map(|pool| pool.disk_set.iter().cloned())
        .collect::<Vec<_>>();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<StorageObjectInfoOrErr<ObjectInfo, Error>>(100);
    let cancellation = tokio_util::sync::CancellationToken::new();
    let walk_cancel = cancellation.clone();
    let bucket_owned = bucket.to_string();
    let walk = async move {
        let results = futures::stream::iter(physical_sets.into_iter().map(|set| {
            let tx = tx.clone();
            let cancellation = walk_cancel.clone();
            let bucket = bucket_owned.clone();
            async move {
                let result = set
                    .walk(
                        cancellation.clone(),
                        &bucket,
                        "",
                        tx,
                        TierReferenceProofWalkOptions {
                            include_free_versions: true,
                            // Total time scales with namespace size; every
                            // underlying drive walk still carries its bounded
                            // progress-stall timeout.
                            walkdir_timeout: Some(Duration::ZERO),
                            ..Default::default()
                        },
                    )
                    .await;
                if result.is_err() {
                    cancellation.cancel();
                }
                result
            }
        }))
        .buffer_unordered(TIER_REFERENCE_PROOF_PHYSICAL_WALK_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
        drop(tx);
        results.into_iter().collect::<Result<Vec<_>>>().map(|_| ())
    };
    let collect = async {
        while let Some(result) = rx.recv().await {
            if let Some(err) = result.err {
                cancellation.cancel();
                return Err(err);
            }
            let Some(object) = result.item else {
                continue;
            };
            match tier_object_blocks_any_target_rebind(&object, targets) {
                Ok(true) => {
                    cancellation.cancel();
                    return Ok(Some(object));
                }
                Ok(false) => {}
                Err(err) => {
                    cancellation.cancel();
                    return Err(Error::other(err));
                }
            }
        }
        Ok(None)
    };
    let (walk_result, collect_result) = tokio::join!(walk, collect);
    match collect_result.map_err(tier_reference_proof_admin_error)? {
        Some(blocker) => Ok(Some(blocker)),
        None => {
            walk_result.map_err(tier_reference_proof_admin_error)?;
            Ok(None)
        }
    }
}

async fn ensure_no_authoritative_lifecycle_references(
    api: &impl TierReferenceProofStore,
    bucket: &str,
    targets: &[TierMutationIntentTarget],
) -> std::result::Result<(), AdminError> {
    match api.lifecycle_config_for_reference_proof(bucket).await {
        Ok(Some(config)) => {
            if let Some(reference) = lifecycle_config_target_reference(&config, targets) {
                return Err(tier_reference_proof_lifecycle_in_use_error(
                    reference.tier_name,
                    bucket,
                    reference.rule_id,
                ));
            }
        }
        Ok(None) => {}
        Err(err) => {
            return Err(tier_reference_proof_admin_error(format!("bucket {bucket} lifecycle config: {err}")));
        }
    }
    Ok(())
}

struct TierLifecycleReference<'a> {
    tier_name: &'a str,
    rule_id: Option<&'a str>,
}

fn lifecycle_config_target_reference<'a>(
    config: &'a BucketLifecycleConfiguration,
    targets: &[TierMutationIntentTarget],
) -> Option<TierLifecycleReference<'a>> {
    for rule in &config.rules {
        let rule_id = rule.id.as_deref();
        if let Some(transitions) = &rule.transitions {
            for transition in transitions {
                let Some(storage_class) = &transition.storage_class else {
                    continue;
                };
                let tier_name = storage_class.as_str();
                if !tier_name.is_empty() && targets.iter().any(|target| target.tier_name == tier_name) {
                    return Some(TierLifecycleReference { tier_name, rule_id });
                }
            }
        }
        if let Some(noncurrent_version_transitions) = &rule.noncurrent_version_transitions {
            for transition in noncurrent_version_transitions {
                let Some(storage_class) = &transition.storage_class else {
                    continue;
                };
                let tier_name = storage_class.as_str();
                if !tier_name.is_empty() && targets.iter().any(|target| target.tier_name == tier_name) {
                    return Some(TierLifecycleReference { tier_name, rule_id });
                }
            }
        }
    }
    None
}

async fn ensure_no_authoritative_persisted_references<S>(
    api: Arc<S>,
    targets: &[TierMutationIntentTarget],
) -> std::result::Result<(), AdminError>
where
    S: TierReferenceProofStore,
{
    ensure_no_authoritative_persisted_references_with(
        api.clone(),
        TIER_DELETE_JOURNAL_PREFIX,
        "tier-delete journal",
        |_object, data| {
            let journal = decode_tier_delete_journal_entry(data).map_err(io::Error::other)?;
            Ok((
                journal.tier_name.clone(),
                tier_persisted_reference_blocks_any_target(&journal.tier_name, journal.backend_identity, targets),
            ))
        },
    )
    .await?;
    ensure_no_authoritative_persisted_references_with(
        api,
        TRANSITION_TRANSACTION_RECORD_PREFIX,
        "transition transaction",
        |object, data| {
            let transaction = decode_transition_transaction_record(object, data).map_err(io::Error::other)?;
            Ok((
                transaction.tier_name.clone(),
                tier_persisted_reference_blocks_any_target(
                    &transaction.tier_name,
                    Some(transaction.backend_fingerprint),
                    targets,
                ),
            ))
        },
    )
    .await
}

async fn ensure_no_authoritative_persisted_references_with<S, F>(
    api: Arc<S>,
    prefix: &str,
    reference_kind: &str,
    blocks_target: F,
) -> std::result::Result<(), AdminError>
where
    S: TierReferenceProofStore,
    F: Fn(&str, &[u8]) -> io::Result<(String, bool)>,
{
    let mut marker = None;
    loop {
        let page = api
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                prefix,
                marker.take(),
                None,
                TIER_REFERENCE_PROOF_LIST_LIMIT,
                false,
                None,
                false,
            )
            .await
            .map_err(tier_reference_proof_admin_error)?;
        for object in &page.objects {
            let data = read_config(api.clone(), &object.name)
                .await
                .map_err(tier_reference_proof_admin_error)?;
            let (tier_name, blocks) = blocks_target(&object.name, &data).map_err(tier_reference_proof_admin_error)?;
            if blocks {
                return Err(tier_reference_proof_persisted_in_use_error(&tier_name, reference_kind, &object.name));
            }
        }
        if !page.is_truncated {
            return Ok(());
        }
        marker = page.next_continuation_token;
        if marker.is_none() {
            return Err(tier_reference_proof_admin_error(io::Error::other(
                "tier persisted reference proof listing is truncated without a next marker",
            )));
        }
    }
}

fn tier_object_blocks_any_target_rebind(object: &ObjectInfo, targets: &[TierMutationIntentTarget]) -> io::Result<bool> {
    if object.transitioned_object.status != rustfs_filemeta::TRANSITION_COMPLETE && !object.transitioned_object.free_version {
        return Ok(false);
    }
    if !targets
        .iter()
        .any(|target| target.tier_name == object.transitioned_object.tier)
    {
        return Ok(false);
    }
    let current_identity = tier_destination_id_from_metadata(&object.user_defined)?;
    Ok(tier_persisted_reference_blocks_any_target(
        &object.transitioned_object.tier,
        current_identity,
        targets,
    ))
}

fn tier_persisted_reference_blocks_any_target(
    tier_name: &str,
    backend_identity: Option<TierDestinationId>,
    targets: &[TierMutationIntentTarget],
) -> bool {
    targets
        .iter()
        .any(|target| tier_persisted_reference_blocks_target(tier_name, backend_identity, target))
}

#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
fn tier_object_blocks_target_rebind(object: &ObjectInfo, target: &TierMutationIntentTarget) -> io::Result<bool> {
    tier_object_blocks_any_target_rebind(object, std::slice::from_ref(target))
}

fn tier_persisted_reference_blocks_target(
    tier_name: &str,
    backend_identity: Option<TierDestinationId>,
    target: &TierMutationIntentTarget,
) -> bool {
    tier_name == target.tier_name
        && match target.new_backend_identity {
            None => true,
            Some(new_backend_identity) => backend_identity != Some(new_backend_identity),
        }
}

fn tier_reference_proof_in_use_error(tier_name: &str, object: &ObjectInfo) -> AdminError {
    let mut err = ERR_TIER_BACKEND_IN_USE.clone();
    let reference_kind = if object.transitioned_object.free_version {
        "free-version ownership"
    } else {
        "transitioned-object"
    };
    err.message = format!(
        "Remote tier {tier_name} still has a {reference_kind} reference, for example {}/{}",
        object.bucket, object.name
    );
    err
}

fn tier_reference_proof_persisted_in_use_error(tier_name: &str, reference_kind: &str, object: &str) -> AdminError {
    let mut err = ERR_TIER_BACKEND_IN_USE.clone();
    err.message = format!("Remote tier {tier_name} still has a {reference_kind} reference, for example {object}");
    err
}

fn tier_reference_proof_lifecycle_in_use_error(tier_name: &str, bucket: &str, rule_id: Option<&str>) -> AdminError {
    let mut err = ERR_TIER_BACKEND_IN_USE.clone();
    err.message = match rule_id {
        Some(rule_id) => format!("Remote tier {tier_name} is still referenced by lifecycle rule {rule_id} in bucket {bucket}"),
        None => format!("Remote tier {tier_name} is still referenced by a lifecycle rule in bucket {bucket}"),
    };
    err
}

fn tier_reference_proof_admin_error(err: impl std::fmt::Display) -> AdminError {
    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
    admin_err.message = format!("Remote tier reference proof failed: {err}");
    admin_err
}

fn tier_mutation_replay_error(err: impl std::fmt::Display) -> io::Error {
    io::Error::other(format!("Remote tier mutation committed replay failed: {err}"))
}

fn tier_mutation_fanout_admin_error(action: &str, err: impl std::fmt::Display) -> AdminError {
    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
    admin_err.message = format!("Remote tier mutation {action} failed: {err}");
    admin_err
}

fn ensure_complete_tier_mutation_commit_peer_set(peer_count: usize, remote_host_count: usize) -> io::Result<()> {
    if peer_count != remote_host_count {
        return Err(tier_mutation_replay_error(
            "cluster endpoint topology has remote hosts without peer commit clients",
        ));
    }
    Ok(())
}

#[async_trait::async_trait]
trait TierMutationPeer: Send + Sync {
    fn peer_label(&self) -> String;
    async fn prepare_tier_mutation(&self, mutation_id: uuid::Uuid, canonical_payload: Bytes) -> Result<PeerTierMutationState>;
    async fn commit_tier_mutation(&self, mutation_id: uuid::Uuid, canonical_payload: Bytes) -> Result<PeerTierMutationState>;
    async fn abort_tier_mutation(
        &self,
        mutation_id: uuid::Uuid,
        canonical_prepare_payload: Bytes,
    ) -> Result<PeerTierMutationState>;
}

#[async_trait::async_trait]
impl TierMutationPeer for PeerRestClient {
    fn peer_label(&self) -> String {
        self.grid_host.clone()
    }

    async fn prepare_tier_mutation(&self, mutation_id: uuid::Uuid, canonical_payload: Bytes) -> Result<PeerTierMutationState> {
        Ok(PeerRestClient::prepare_tier_mutation(self, mutation_id, canonical_payload)
            .await?
            .state)
    }

    async fn commit_tier_mutation(&self, mutation_id: uuid::Uuid, canonical_payload: Bytes) -> Result<PeerTierMutationState> {
        Ok(PeerRestClient::commit_tier_mutation(self, mutation_id, canonical_payload)
            .await?
            .state)
    }

    async fn abort_tier_mutation(
        &self,
        mutation_id: uuid::Uuid,
        canonical_prepare_payload: Bytes,
    ) -> Result<PeerTierMutationState> {
        Ok(PeerRestClient::abort_tier_mutation(self, mutation_id, canonical_prepare_payload)
            .await?
            .state)
    }
}

#[cfg(test)]
tokio::task_local! {
    static TIER_MUTATION_TEST_PEERS: Vec<Arc<dyn TierMutationPeer>>;
}

async fn remote_tier_mutation_peers() -> io::Result<Vec<Arc<dyn TierMutationPeer>>> {
    #[cfg(test)]
    if let Ok(peers) = TIER_MUTATION_TEST_PEERS.try_with(|peers| peers.clone()) {
        return Ok(peers);
    }
    if !runtime_sources::setup_is_dist_erasure().await {
        return Ok(Vec::new());
    }
    let Some(endpoints) = runtime_sources::endpoint_pools() else {
        return Err(tier_mutation_replay_error("cluster endpoint topology is not initialized"));
    };
    remote_tier_mutation_peers_from_topology(endpoints).await
}

async fn remote_tier_mutation_peers_from_topology(endpoints: EndpointServerPools) -> io::Result<Vec<Arc<dyn TierMutationPeer>>> {
    let (peers, _, remote_topology_hosts) = PeerRestClient::new_clients_with_topology(endpoints).await;
    let peers = peers
        .into_iter()
        .flatten()
        .map(|peer| Arc::new(peer) as Arc<dyn TierMutationPeer>)
        .collect::<Vec<_>>();
    ensure_complete_tier_mutation_commit_peer_set(peers.len(), remote_topology_hosts.len())?;
    Ok(peers)
}

async fn prepare_tier_mutation_peers(
    mutation_id: uuid::Uuid,
    peers: Vec<Arc<dyn TierMutationPeer>>,
    intent: &TierMutationIntent,
) -> std::result::Result<Vec<Arc<dyn TierMutationPeer>>, TierMutationPrepareFailure> {
    let payload = Bytes::from(intent.encode().map_err(|err| TierMutationPrepareFailure {
        error: tier_mutation_fanout_admin_error("prepare", err),
        prepared_peers: Vec::new(),
    })?);
    let peers = peers.into_iter().map(|peer| (peer.peer_label(), peer)).collect::<Vec<_>>();
    let fanout_deadline = tier_mutation_peer_fanout_deadline(Some(intent.expires_at_unix_nanos));
    let (started, mut outcomes) = run_bounded_tier_mutation_peer_calls(&peers, fanout_deadline, |index, peer| {
        let payload = payload.clone();
        async move {
            let deadline = Instant::now() + TIER_MUTATION_PEER_RPC_TIMEOUT;
            let result = timeout_at(deadline, peer.prepare_tier_mutation(mutation_id, payload)).await;
            (index, result)
        }
        .boxed()
    })
    .await;

    let mut prepared = Vec::with_capacity(peers.len());
    let mut failures = Vec::new();
    for (index, (label, peer)) in peers.into_iter().enumerate() {
        match outcomes[index].take() {
            Some(result) => match result {
                Ok(Ok(PeerTierMutationState::Prepared)) => prepared.push(peer),
                Ok(Ok(PeerTierMutationState::Committed)) => {}
                Ok(Ok(state)) => failures.push(format!("peer {label} returned unexpected state {state:?}")),
                Ok(Err(err)) => {
                    if tier_mutation_prepare_was_definitely_rejected(&err) {
                        failures.push(format!("peer {label}: {err}"));
                        continue;
                    }
                    // The request may have reached the peer and installed its
                    // durable Prepared block before the response failed (for
                    // example while draining an in-flight lease). Include it in
                    // abort fanout so ambiguous prepare outcomes fail closed
                    // without stranding a runtime block.
                    prepared.push(peer);
                    failures.push(format!("peer {label}: {err}"));
                }
                Err(_) => {
                    prepared.push(peer);
                    failures.push(format!("peer {label}: request timed out"));
                }
            },
            None if started[index] => {
                // The fanout-wide deadline cancelled an in-flight request.
                // Its durable result is unknown, so compensating Abort must
                // include this peer.
                prepared.push(peer);
                failures.push(format!("peer {label}: fanout deadline expired with request in flight"));
            }
            None => failures.push(format!("peer {label}: fanout deadline expired before request start")),
        }
    }
    if failures.is_empty() {
        Ok(prepared)
    } else {
        Err(TierMutationPrepareFailure {
            error: tier_mutation_fanout_admin_error("prepare", failures.join("; ")),
            prepared_peers: prepared,
        })
    }
}

fn tier_mutation_prepare_was_definitely_rejected(err: &Error) -> bool {
    tier_mutation_error_is_definitely_rejected(err)
}

struct TierMutationPrepareFailure {
    error: AdminError,
    prepared_peers: Vec<Arc<dyn TierMutationPeer>>,
}

type TierMutationPeerCallOutcome = std::result::Result<Result<PeerTierMutationState>, tokio::time::error::Elapsed>;

fn tier_mutation_peer_fanout_deadline(intent_expiry_unix_nanos: Option<i64>) -> Instant {
    let mut budget = TIER_MUTATION_PEER_FANOUT_TIMEOUT;
    if let Some(expires_at) = intent_expiry_unix_nanos {
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        let remaining = i128::from(expires_at).saturating_sub(now);
        let remaining = if remaining <= 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(u64::try_from(remaining).unwrap_or(u64::MAX))
        };
        budget = budget.min(remaining);
    }
    Instant::now() + budget
}

async fn run_bounded_tier_mutation_peer_calls<F>(
    peers: &[(String, Arc<dyn TierMutationPeer>)],
    fanout_deadline: Instant,
    make_call: F,
) -> (Vec<bool>, Vec<Option<TierMutationPeerCallOutcome>>)
where
    F: Fn(usize, Arc<dyn TierMutationPeer>) -> futures::future::BoxFuture<'static, (usize, TierMutationPeerCallOutcome)>,
{
    let mut started = vec![false; peers.len()];
    let mut outcomes = std::iter::repeat_with(|| None).take(peers.len()).collect::<Vec<_>>();
    let mut pending = FuturesUnordered::new();
    let mut next = 0;
    while next < peers.len() && pending.len() < TIER_MUTATION_PEER_FANOUT_CONCURRENCY && Instant::now() < fanout_deadline {
        started[next] = true;
        pending.push(make_call(next, peers[next].1.clone()));
        next += 1;
    }
    while !pending.is_empty() {
        let completed = match timeout_at(fanout_deadline, pending.next()).await {
            Ok(Some(completed)) => completed,
            Ok(None) | Err(_) => break,
        };
        outcomes[completed.0] = Some(completed.1);
        if next < peers.len() && Instant::now() < fanout_deadline {
            started[next] = true;
            pending.push(make_call(next, peers[next].1.clone()));
            next += 1;
        }
    }
    (started, outcomes)
}

async fn abort_tier_mutation_peers(intent: &TierMutationIntent, peers: Vec<Arc<dyn TierMutationPeer>>) -> io::Result<()> {
    let prepared = intent.original_prepared().map_err(io::Error::other)?;
    let payload = Bytes::from(prepared.encode().map_err(io::Error::other)?);
    let mutation_id = intent.mutation_id;
    let peers = peers.into_iter().map(|peer| (peer.peer_label(), peer)).collect::<Vec<_>>();
    let fanout_deadline = tier_mutation_peer_fanout_deadline(None);
    let (started, mut outcomes) = run_bounded_tier_mutation_peer_calls(&peers, fanout_deadline, |index, peer| {
        let payload = payload.clone();
        async move {
            let deadline = Instant::now() + TIER_MUTATION_PEER_RPC_TIMEOUT;
            let result = timeout_at(deadline, peer.abort_tier_mutation(mutation_id, payload)).await;
            (index, result)
        }
        .boxed()
    })
    .await;
    let mut failures = Vec::new();
    for (index, (label, _)) in peers.iter().enumerate() {
        match outcomes[index].take() {
            Some(result) => match result {
                Ok(Ok(PeerTierMutationState::Aborted)) => {}
                Ok(Ok(state)) => {
                    failures.push(format!("peer {label} returned unexpected abort state {state:?}"));
                }
                Ok(Err(err)) => failures.push(format!("peer {label}: {err}")),
                Err(_) => failures.push(format!("peer {label}: request timed out")),
            },
            None if started[index] => failures.push(format!("peer {label}: fanout deadline expired with request in flight")),
            None => failures.push(format!("peer {label}: fanout deadline expired before request start")),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(tier_mutation_replay_error(failures.join("; ")))
    }
}

async fn commit_tier_mutation_peers(
    mutation_id: uuid::Uuid,
    peers: Vec<Arc<dyn TierMutationPeer>>,
    committed_config_etag: &str,
) -> io::Result<()> {
    let payload = Bytes::copy_from_slice(committed_config_etag.as_bytes());
    let peers = peers.into_iter().map(|peer| (peer.peer_label(), peer)).collect::<Vec<_>>();
    let fanout_deadline = tier_mutation_peer_fanout_deadline(None);
    let (started, mut outcomes) = run_bounded_tier_mutation_peer_calls(&peers, fanout_deadline, |index, peer| {
        let payload = payload.clone();
        async move {
            let deadline = Instant::now() + TIER_MUTATION_PEER_RPC_TIMEOUT;
            let result = timeout_at(deadline, peer.commit_tier_mutation(mutation_id, payload)).await;
            (index, result)
        }
        .boxed()
    })
    .await;
    let mut failures = Vec::new();
    for (index, (label, _)) in peers.iter().enumerate() {
        match outcomes[index].take() {
            Some(result) => match result {
                Ok(Ok(PeerTierMutationState::Committed)) => {}
                Ok(Ok(state)) => failures.push(format!("peer {label} returned unexpected state {state:?}")),
                Ok(Err(err)) => failures.push(format!("peer {label}: {err}")),
                Err(_) => failures.push(format!("peer {label}: request timed out")),
            },
            None if started[index] => failures.push(format!("peer {label}: fanout deadline expired with request in flight")),
            None => failures.push(format!("peer {label}: fanout deadline expired before request start")),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(tier_mutation_replay_error(failures.join("; ")))
    }
}

fn tier_config_digest(bytes: &[u8]) -> TierMutationDigest {
    Sha256::digest(bytes).into()
}

pub(crate) fn tier_config_candidate_digest(candidate: &TierConfigMgr) -> io::Result<TierMutationDigest> {
    let bytes = encode_external_tiering_config_blob(candidate)?;
    Ok(tier_config_digest(&bytes))
}

fn tier_mutation_intent_expiry_unix_nanos() -> i64 {
    let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
    let ttl = i128::try_from(TIER_MUTATION_INTENT_TTL.as_nanos()).unwrap_or(i128::MAX);
    i64::try_from(now.saturating_add(ttl)).unwrap_or(i64::MAX)
}

fn build_coordinator_tier_mutation_intent(
    kind: TierMutationIntentKind,
    old_config_etag: Option<String>,
    candidate: &TierConfigMgr,
    affected_targets: Vec<TierMutationIntentTarget>,
) -> io::Result<Option<TierMutationIntent>> {
    if affected_targets.is_empty() || (old_config_etag.is_none() && kind != TierMutationIntentKind::Add) {
        return Ok(None);
    }
    Ok(Some(TierMutationIntent {
        mutation_id: uuid::Uuid::new_v4(),
        revision: 1,
        kind,
        state: TierMutationIntentState::Prepared,
        old_config_etag,
        committed_config_etag: None,
        candidate_digest: tier_config_candidate_digest(candidate)?,
        affected_targets,
        expires_at_unix_nanos: tier_mutation_intent_expiry_unix_nanos(),
    }))
}

async fn save_coordinator_tier_mutation_intent<S>(api: Arc<S>, intent: Option<&TierMutationIntent>) -> io::Result<()>
where
    S: TierReferenceProofStore,
{
    if let Some(intent) = intent {
        let _mutation_guard = acquire_tier_mutation_mutex(intent.mutation_id).await;
        save_tier_coordinator_mutation_intent_record_if_absent(api, intent)
            .await
            .map_err(io::Error::other)?;
    }
    Ok(())
}

async fn commit_coordinator_tier_mutation_intent<S>(
    api: Arc<S>,
    intent: Option<&TierMutationIntent>,
    committed_config_etag: &str,
) -> io::Result<()>
where
    S: TierReferenceProofStore,
{
    if let Some(intent) = intent {
        let _mutation_guard = acquire_tier_mutation_mutex(intent.mutation_id).await;
        advance_tier_coordinator_mutation_intent_record_idempotent(
            api,
            intent.mutation_id,
            TierMutationIntentState::Committed,
            Some(committed_config_etag.to_string()),
        )
        .await
        .map_err(io::Error::other)?;
    }
    Ok(())
}

async fn abort_coordinator_tier_mutation_intent<S>(api: Arc<S>, intent: Option<&TierMutationIntent>) -> bool
where
    S: TierReferenceProofStore,
{
    let Some(intent) = intent else {
        return true;
    };
    let _mutation_guard = acquire_tier_mutation_mutex(intent.mutation_id).await;
    match advance_tier_coordinator_mutation_intent_record_idempotent(
        api,
        intent.mutation_id,
        TierMutationIntentState::Aborted,
        None,
    )
    .await
    {
        Ok(_) => true,
        Err(err) => {
            warn!(
                event = "tier_mutation_abort",
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_TIER,
                result = "coordinator_abort_failed",
                mutation_id = %intent.mutation_id,
                error = ?err,
                "tier mutation abort incomplete"
            );
            false
        }
    }
}

async fn abort_prepared_tier_mutation<S>(
    handle: &Arc<RwLock<TierConfigMgr>>,
    api: Arc<S>,
    intent: Option<&TierMutationIntent>,
    prepared_peers: Vec<Arc<dyn TierMutationPeer>>,
) -> bool
where
    S: TierReferenceProofStore,
{
    let Some(intent) = intent else {
        return true;
    };
    if let Err(err) = abort_tier_mutation_peers(intent, prepared_peers).await {
        warn!(
            event = "tier_mutation_abort",
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_TIER,
            result = "peer_abort_failed",
            mutation_id = %intent.mutation_id,
            error = ?err,
            "tier mutation abort incomplete"
        );
        // The coordinator record remains Prepared so recovery can retry the
        // genuinely ambiguous peer cleanup. The local data path must not stay
        // fenced merely because a remote Abort response was lost.
        if let Err(clear_err) = TierConfigMgr::clear_prepared_mutation_intent_block(handle, intent.mutation_id).await {
            warn!(
                event = "tier_mutation_abort",
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_TIER,
                result = "runtime_fence_clear_failed",
                mutation_id = %intent.mutation_id,
                error = ?clear_err,
                "tier mutation abort incomplete"
            );
        }
        return false;
    }
    let coordinator_aborted = abort_coordinator_tier_mutation_intent(api, Some(intent)).await;
    let runtime_cleared = if let Err(err) = TierConfigMgr::clear_prepared_mutation_intent_block(handle, intent.mutation_id).await
    {
        warn!(
            event = "tier_mutation_abort",
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_TIER,
            result = "runtime_fence_clear_failed",
            mutation_id = %intent.mutation_id,
            error = ?err,
            "tier mutation abort incomplete"
        );
        false
    } else {
        true
    };
    coordinator_aborted && runtime_cleared
}

fn committed_tier_mutation_intent(
    intent: Option<&TierMutationIntent>,
    committed_config_etag: &str,
) -> io::Result<Option<TierMutationIntent>> {
    let Some(intent) = intent else {
        return Ok(None);
    };
    let mut committed = intent.clone();
    committed
        .advance(TierMutationIntentState::Committed, Some(committed_config_etag.to_string()))
        .map_err(io::Error::other)?;
    Ok(Some(committed))
}

async fn apply_tier_candidate_mutation(
    mutation: TierCandidateMutation,
    candidate: &mut TierConfigMgr,
    deadline: Instant,
) -> std::result::Result<Option<String>, AdminError> {
    if matches!(&mutation, TierCandidateMutation::Add(_, _) | TierCandidateMutation::Edit(_, _)) {
        // Add/Edit validation performs its own deadline-aware cleanup. Do not
        // wrap it in an outer timeout that would cancel an uncertain probe.
        return mutation.apply(candidate, Some(deadline)).await;
    }
    match timeout_at(deadline, mutation.apply(candidate, None)).await {
        Ok(result) => result,
        Err(_) => {
            let mut err = ERR_TIER_BACKEND_IN_USE.clone();
            err.message = "Timed out validating the remote tier mutation".to_string();
            Err(err)
        }
    }
}

async fn prevalidate_tier_candidate_mutation(
    mut candidate: TierConfigMgr,
    version: Option<String>,
    mutation: TierCandidateMutation,
) -> std::result::Result<PrevalidatedTierCandidateMutation, TierConfigUpdateError> {
    let kind = mutation.intent_kind();
    if version.is_none() && !candidate.tiers.is_empty() && kind != TierMutationIntentKind::Add {
        return Err(TierConfigUpdateError::Load(io::Error::other(
            "tier configuration mutation requires an existing config ETag",
        )));
    }
    let explicit_tier_name = mutation.explicit_tier_name().map(str::to_string);
    let force = mutation.force();
    let current = TierConfigMgr {
        driver_cache: HashMap::new(),
        tiers: candidate
            .tiers
            .iter()
            .map(|(tier_name, config)| (tier_name.clone(), config.clone_with_credentials()))
            .collect(),
        last_refreshed_at: candidate.last_refreshed_at,
    };
    let validation_deadline = Instant::now() + TIER_REMOTE_VALIDATION_TIMEOUT;
    let driver_tier = apply_tier_candidate_mutation(mutation, &mut candidate, validation_deadline)
        .await
        .map_err(TierConfigUpdateError::Mutation)?;
    let candidate_digest = tier_config_candidate_digest(&candidate).map_err(TierConfigUpdateError::Save)?;
    Ok(PrevalidatedTierCandidateMutation {
        candidate,
        current,
        version,
        kind,
        explicit_tier_name,
        force,
        driver_tier,
        candidate_digest,
    })
}

fn changed_tier_names(current: &TierConfigMgr, replacement: &TierConfigMgr) -> HashSet<String> {
    let mut changed = HashSet::new();
    for (tier_name, current_config) in &current.tiers {
        if replacement
            .tiers
            .get(tier_name)
            .is_none_or(|replacement_config| !tier_configs_match(tier_name, current_config, replacement_config))
        {
            changed.insert(tier_name.clone());
        }
    }
    changed.extend(
        replacement
            .tiers
            .keys()
            .filter(|tier_name| !current.tiers.contains_key(*tier_name))
            .cloned(),
    );
    changed
}

fn replaced_tier_destinations(
    current: &TierConfigMgr,
    replacement: &TierConfigMgr,
) -> std::result::Result<HashMap<String, TierConfig>, AdminError> {
    let mut replaced = HashMap::new();
    for (tier_name, current_config) in &current.tiers {
        let changed = match replacement.tiers.get(tier_name) {
            Some(replacement_config) => {
                if matches!(current_config.tier_type, TierType::Wasabi)
                    != matches!(replacement_config.tier_type, TierType::Wasabi)
                {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message =
                        "Changing an existing remote tier to or from Wasabi is not supported; add a new tier instead".to_string();
                    return Err(admin_err);
                }
                let current_identity = tier_backend_identity(current_config).map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })?;
                let replacement_identity = tier_backend_identity(replacement_config).map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })?;
                current_identity != replacement_identity
            }
            None => true,
        };
        if changed {
            replaced.insert(tier_name.clone(), current_config.clone_with_credentials());
        }
    }
    Ok(replaced)
}

fn encode_tier_backend_identity(
    tier_type: &str,
    endpoint: &str,
    bucket: &str,
    prefix: &str,
    region: &str,
    routing_account: &str,
) -> io::Result<TierDestinationId> {
    let encoded = rmp_serde::to_vec(&(
        TIER_BACKEND_IDENTITY_VERSION,
        tier_type,
        endpoint,
        bucket,
        prefix,
        region,
        routing_account,
    ))
    .map_err(|err| io::Error::other(format!("serialize tier backend identity failed: {err}")))?;
    Ok(Sha256::digest(encoded).into())
}

fn tier_backend_identity(config: &TierConfig) -> io::Result<TierDestinationId> {
    if matches!(config.tier_type, TierType::Wasabi) {
        let wasabi = config
            .wasabi
            .as_ref()
            .ok_or_else(|| io::Error::other("tier backend identity payload is missing"))?;
        return encode_tier_backend_identity(
            "s3",
            &wasabi.canonical_endpoint()?,
            &wasabi.bucket,
            normalized_tier_prefix(&config.tier_type, &wasabi.prefix),
            &wasabi.region,
            "",
        );
    }

    let (tier_type, endpoint, bucket, prefix, region, routing_account) = match config.tier_type {
        TierType::S3 => config.s3.as_ref().map(|value| {
            (
                "s3",
                value.endpoint.as_str(),
                value.bucket.as_str(),
                value.prefix.as_str(),
                value.region.as_str(),
                "",
            )
        }),
        TierType::Wasabi => None,
        TierType::RustFS => config.rustfs.as_ref().map(|value| {
            (
                "rustfs",
                value.endpoint.as_str(),
                value.bucket.as_str(),
                value.prefix.as_str(),
                value.region.as_str(),
                "",
            )
        }),
        TierType::MinIO => config.minio.as_ref().map(|value| {
            (
                "minio",
                value.endpoint.as_str(),
                value.bucket.as_str(),
                value.prefix.as_str(),
                value.region.as_str(),
                "",
            )
        }),
        TierType::Aliyun => config.aliyun.as_ref().map(|value| {
            (
                "aliyun",
                value.endpoint.as_str(),
                value.bucket.as_str(),
                value.prefix.as_str(),
                value.region.as_str(),
                "",
            )
        }),
        TierType::Tencent => config.tencent.as_ref().map(|value| {
            (
                "tencent",
                value.endpoint.as_str(),
                value.bucket.as_str(),
                value.prefix.as_str(),
                value.region.as_str(),
                "",
            )
        }),
        TierType::Huaweicloud => config.huaweicloud.as_ref().map(|value| {
            (
                "huaweicloud",
                value.endpoint.as_str(),
                value.bucket.as_str(),
                value.prefix.as_str(),
                value.region.as_str(),
                "",
            )
        }),
        TierType::Azure => config.azure.as_ref().map(|value| {
            (
                "azure",
                value.endpoint.as_str(),
                value.bucket.as_str(),
                value.prefix.as_str(),
                value.region.as_str(),
                value.access_key.as_str(),
            )
        }),
        TierType::GCS => config.gcs.as_ref().map(|value| {
            (
                "gcs",
                value.endpoint.as_str(),
                value.bucket.as_str(),
                value.prefix.as_str(),
                value.region.as_str(),
                "",
            )
        }),
        TierType::R2 => config.r2.as_ref().map(|value| {
            (
                "r2",
                value.endpoint.as_str(),
                value.bucket.as_str(),
                value.prefix.as_str(),
                value.region.as_str(),
                "",
            )
        }),
        TierType::Unsupported => None,
    }
    .ok_or_else(|| io::Error::other("tier backend identity payload is missing"))?;
    let prefix = normalized_tier_prefix(&config.tier_type, prefix);
    encode_tier_backend_identity(tier_type, endpoint, bucket, prefix, region, routing_account)
}

pub(crate) fn tier_destination_id_from_metadata(metadata: &HashMap<String, String>) -> io::Result<Option<TierDestinationId>> {
    let Some(encoded) = rustfs_utils::http::metadata_compat::get_consistent_str(
        metadata,
        rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
    ) else {
        if rustfs_utils::http::metadata_compat::contains_key_str(
            metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
        ) {
            return Err(io::Error::other(
                "transition tier backend identity compatibility keys conflict or are empty",
            ));
        }
        return Ok(None);
    };
    if encoded.len() != 64 {
        return Err(io::Error::other("transition tier backend identity has an invalid length"));
    }
    let mut identity = [0_u8; 32];
    for (index, byte) in identity.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16)
            .map_err(|_| io::Error::other("transition tier backend identity is not valid hexadecimal"))?;
    }
    Ok(Some(identity))
}

fn normalized_tier_prefix<'a>(tier_type: &TierType, prefix: &'a str) -> &'a str {
    match tier_type {
        TierType::S3 | TierType::Wasabi => prefix.trim_matches('/'),
        TierType::RustFS
        | TierType::MinIO
        | TierType::Aliyun
        | TierType::Tencent
        | TierType::Huaweicloud
        | TierType::Azure
        | TierType::GCS
        | TierType::R2 => prefix.strip_suffix('/').unwrap_or(prefix),
        TierType::Unsupported => prefix,
    }
}

fn tier_exact_get_delete(config: &TierConfig) -> bool {
    ProviderVersionCapabilities::for_tier_type(&config.tier_type.as_lowercase()).exact_get_delete
}

struct TierDriverGeneration {
    tier_name: Arc<str>,
    tier_config: TierConfig,
    generation: DriverRevision,
    // Process-local only: this may reflect credential changes and must never be persisted or logged.
    config_fingerprint: TierDriverFingerprint,
    // Durable cleanup identity: routing fields only, with all credentials excluded.
    backend_identity: TierDestinationId,
    exact_get_delete: bool,
    driver: SharedWarmBackend,
    reconciler: tokio::sync::OnceCell<
        Option<Arc<dyn crate::services::tier::warm_backend::TransitionCandidateReconciler + Send + Sync + 'static>>,
    >,
    accepting: AtomicBool,
    active_leases: AtomicUsize,
    drained: tokio::sync::Notify,
}

struct SharedWarmBackendProxy(SharedWarmBackend);

#[async_trait::async_trait]
impl WarmBackend for SharedWarmBackendProxy {
    async fn validate(&self) -> io::Result<()> {
        self.0.validate().await
    }

    fn validate_remote_version_id(&self, remote_version_id: &str) -> io::Result<()> {
        self.0.validate_remote_version_id(remote_version_id)
    }

    async fn put(&self, object: &str, r: rustfs_s3_client::transition_api::ReaderImpl, length: i64) -> io::Result<String> {
        self.0.put(object, r, length).await
    }

    async fn put_with_meta(
        &self,
        object: &str,
        r: rustfs_s3_client::transition_api::ReaderImpl,
        length: i64,
        meta: HashMap<String, String>,
    ) -> io::Result<String> {
        self.0.put_with_meta(object, r, length, meta).await
    }

    async fn get(
        &self,
        object: &str,
        rv: &str,
        opts: crate::services::tier::warm_backend::WarmBackendGetOpts,
    ) -> io::Result<rustfs_s3_client::transition_api::ReadCloser> {
        self.0.get(object, rv, opts).await
    }

    async fn remove(&self, object: &str, rv: &str) -> io::Result<()> {
        self.0.remove(object, rv).await
    }

    async fn remove_exact(&self, object: &str, rv: &str) -> io::Result<()> {
        self.0.remove_exact(object, rv).await
    }

    async fn probe_transition_candidate(&self, object: &str) -> io::Result<TransitionCandidateProbe> {
        self.0.probe_transition_candidate(object).await
    }

    async fn probe_transition_version(&self, object: &str, remote_version_id: &str) -> io::Result<TransitionCandidateProbe> {
        self.0.probe_transition_version(object, remote_version_id).await
    }

    async fn in_use(&self) -> io::Result<bool> {
        self.0.in_use().await
    }
}

pub(crate) struct TierOperationLease {
    inner: Arc<TierDriverGeneration>,
    runtime: Arc<Mutex<TierDriverRuntime>>,
}

impl TierOperationLease {
    fn try_new(
        inner: Arc<TierDriverGeneration>,
        runtime: Arc<Mutex<TierDriverRuntime>>,
    ) -> std::result::Result<Self, AdminError> {
        inner
            .active_leases
            .try_update(Ordering::AcqRel, Ordering::Acquire, |active| active.checked_add(1))
            .map_err(|_| {
                let mut err = ERR_TIER_INVALID_CONFIG.clone();
                err.message = "Remote tier operation lease capacity exhausted".to_string();
                err
            })?;
        Ok(Self { inner, runtime })
    }

    pub(crate) fn try_clone(&self) -> std::result::Result<Self, AdminError> {
        Self::try_new(self.inner.clone(), self.runtime.clone())
    }
}

impl Drop for TierOperationLease {
    fn drop(&mut self) {
        let result = self
            .inner
            .active_leases
            .try_update(Ordering::AcqRel, Ordering::Acquire, |active| active.checked_sub(1));
        match result {
            Ok(1) => self.inner.drained.notify_one(),
            Ok(_) => {}
            Err(_) => {
                error!(
                    tier = self.tier_name(),
                    tier_generation = self.generation(),
                    "tier operation lease counter underflow"
                );
            }
        }
    }
}

impl Deref for TierOperationLease {
    type Target = dyn WarmBackend + Send + Sync + 'static;

    fn deref(&self) -> &Self::Target {
        self.inner.driver.as_ref()
    }
}

impl TierOperationLease {
    pub(crate) fn tier_name(&self) -> &str {
        &self.inner.tier_name
    }

    pub(crate) fn generation(&self) -> DriverRevision {
        self.inner.generation
    }

    pub(crate) fn backend_identity(&self) -> TierDestinationId {
        self.inner.backend_identity
    }

    pub(crate) async fn probe_transition_candidate_for(
        &self,
        object: &str,
        transaction_id: uuid::Uuid,
    ) -> io::Result<TransitionCandidateProbe> {
        let Some(reconciler) = self
            .inner
            .reconciler
            .get_or_try_init(|| async {
                crate::services::tier::warm_backend::new_transition_candidate_reconciler(&self.inner.tier_config)
                    .await
                    .map(|reconciler| reconciler.map(Arc::from))
            })
            .await
            .map_err(|err| io::Error::other(err.message))?
        else {
            return self.inner.driver.probe_transition_candidate(object).await;
        };
        reconciler
            .probe_transition_candidate_for(
                object,
                crate::services::tier::warm_backend::TransitionCandidateIdentity {
                    transaction_id,
                    destination_id: self.backend_identity(),
                },
            )
            .await
    }

    pub(crate) fn validate_remote_version_id(&self, remote_version_id: &str) -> io::Result<()> {
        self.inner.driver.validate_remote_version_id(remote_version_id)?;
        if !remote_version_id.is_empty() && !self.inner.exact_get_delete {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "remote tier does not support exact versioned GET and DELETE",
            ));
        }
        Ok(())
    }

    pub(crate) async fn probe_transition_version(
        &self,
        object: &str,
        remote_version_id: &str,
    ) -> io::Result<TransitionCandidateProbe> {
        self.validate_remote_version_id(remote_version_id)?;
        self.inner.driver.probe_transition_version(object, remote_version_id).await
    }

    pub(crate) fn is_current_generation(&self) -> bool {
        lock_unpoisoned(&self.runtime)
            .generations
            .get(self.tier_name())
            .is_some_and(|current| {
                current.generation == self.generation()
                    && current.accepting.load(Ordering::Acquire)
                    && Arc::ptr_eq(current, &self.inner)
            })
    }

    #[cfg(test)]
    async fn is_current(&self, handle: &Arc<RwLock<TierConfigMgr>>) -> bool {
        let manager = handle.read().await;
        let Some(runtime) = registered_tier_driver_runtime(&manager) else {
            return false;
        };
        if !Arc::ptr_eq(&runtime, &self.runtime) {
            return false;
        }
        self.is_current_generation()
    }
}

impl TierDriverGeneration {
    async fn wait_for_no_active_leases(&self) {
        loop {
            let notified = self.drained.notified();
            if self.active_leases.load(Ordering::Acquire) == 0 {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierConfigMgr {
    #[serde(rename = "Tiers")]
    tiers: BTreeMap<String, ExternalTierConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierConfig {
    #[serde(rename = "Version")]
    version: String,
    #[serde(rename = "Type")]
    tier_type: i32,
    #[serde(rename = "Name")]
    name: String,
    #[serde(rename = "S3")]
    s3: Option<ExternalTierS3>,
    #[serde(rename = "Azure")]
    azure: Option<ExternalTierAzure>,
    #[serde(rename = "GCS")]
    gcs: Option<ExternalTierGcs>,
    #[serde(rename = "MinIO", alias = "Compatible")]
    compatible_backend: Option<ExternalTierCompatible>,
    #[serde(rename = "XTierType", skip_serializing_if = "Option::is_none")]
    tier_type_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierS3 {
    #[serde(rename = "Endpoint")]
    endpoint: String,
    #[serde(rename = "AccessKey")]
    access_key: String,
    #[serde(rename = "SecretKey")]
    secret_key: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "Region")]
    region: String,
    #[serde(rename = "StorageClass")]
    storage_class: String,
    #[serde(rename = "AWSRole")]
    aws_role: bool,
    #[serde(rename = "AWSRoleWebIdentityTokenFile")]
    aws_role_web_identity_token_file: String,
    #[serde(rename = "AWSRoleARN")]
    aws_role_arn: String,
    #[serde(rename = "AWSRoleSessionName")]
    aws_role_session_name: String,
    #[serde(rename = "AWSRoleDurationSeconds")]
    aws_role_duration_seconds: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalServicePrincipalAuth {
    #[serde(rename = "TenantID")]
    tenant_id: String,
    #[serde(rename = "ClientID")]
    client_id: String,
    #[serde(rename = "ClientSecret")]
    client_secret: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierAzure {
    #[serde(rename = "Endpoint")]
    endpoint: String,
    #[serde(rename = "AccountName")]
    account_name: String,
    #[serde(rename = "AccountKey")]
    account_key: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "Region")]
    region: String,
    #[serde(rename = "StorageClass")]
    storage_class: String,
    #[serde(rename = "SPAuth")]
    sp_auth: ExternalServicePrincipalAuth,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierGcs {
    #[serde(rename = "Endpoint")]
    endpoint: String,
    #[serde(rename = "Creds")]
    creds: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "Region")]
    region: String,
    #[serde(rename = "StorageClass")]
    storage_class: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
struct ExternalTierCompatible {
    #[serde(rename = "Endpoint")]
    endpoint: String,
    #[serde(rename = "AccessKey")]
    access_key: String,
    #[serde(rename = "SecretKey")]
    secret_key: String,
    #[serde(rename = "Bucket")]
    bucket: String,
    #[serde(rename = "Prefix")]
    prefix: String,
    #[serde(rename = "Region")]
    region: String,
}

fn decode_external_gcs_credentials(credentials: &str) -> io::Result<String> {
    if serde_json::from_str::<serde_json::Value>(credentials).is_ok() {
        return Ok(credentials.to_string());
    }
    // MinIO config and madmin AddTier payloads carry URL-safe-base64 credentials,
    // while the existing RustFS v2 disk format stores raw JSON for rolling upgrades.
    let decoded = base64_simd::STANDARD
        .decode_to_vec(credentials.as_bytes())
        .or_else(|_| base64_simd::STANDARD_NO_PAD.decode_to_vec(credentials.as_bytes()))
        .or_else(|_| base64_simd::URL_SAFE.decode_to_vec(credentials.as_bytes()))
        .or_else(|_| base64_simd::URL_SAFE_NO_PAD.decode_to_vec(credentials.as_bytes()))
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "tier config contains invalid GCS credentials encoding"))?;
    String::from_utf8(decoded)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "tier config contains non-UTF-8 GCS credentials JSON"))
}

fn tier_config_path(file: &str) -> String {
    format!("{}{}{}", CONFIG_PREFIX, SLASH_SEPARATOR, file)
}

fn tier_config_lock_path() -> String {
    format!("{}.lock", tier_config_path(TIER_CONFIG_FILE))
}

fn tier_hint_for_type(tier_type: TierType) -> Option<&'static str> {
    match tier_type {
        TierType::RustFS => Some("rustfs"),
        TierType::Wasabi => Some("wasabi"),
        TierType::Aliyun => Some("aliyun"),
        TierType::Tencent => Some("tencent"),
        TierType::Huaweicloud => Some("huaweicloud"),
        TierType::R2 => Some("r2"),
        _ => None,
    }
}

fn tier_type_from_hint(hint: Option<&str>) -> Option<TierType> {
    match hint {
        Some("rustfs") => Some(TierType::RustFS),
        Some("aliyun") => Some(TierType::Aliyun),
        Some("tencent") => Some(TierType::Tencent),
        Some("huaweicloud") => Some(TierType::Huaweicloud),
        Some("r2") => Some(TierType::R2),
        _ => None,
    }
}

fn external_tier_s3_from_internal(s3: &crate::services::tier::tier_config::TierS3) -> ExternalTierS3 {
    ExternalTierS3 {
        endpoint: s3.endpoint.clone(),
        access_key: s3.access_key.clone(),
        secret_key: s3.secret_key.clone(),
        bucket: s3.bucket.clone(),
        prefix: s3.prefix.clone(),
        region: s3.region.clone(),
        storage_class: s3.storage_class.clone(),
        aws_role: s3.aws_role,
        aws_role_web_identity_token_file: s3.aws_role_web_identity_token_file.clone(),
        aws_role_arn: s3.aws_role_arn.clone(),
        aws_role_session_name: s3.aws_role_session_name.clone(),
        aws_role_duration_seconds: s3.aws_role_duration_seconds,
    }
}

fn external_tier_s3_from_compatible_payload(
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
    prefix: String,
    region: String,
) -> ExternalTierS3 {
    ExternalTierS3 {
        endpoint,
        access_key,
        secret_key,
        bucket,
        prefix,
        region,
        storage_class: String::new(),
        aws_role: false,
        aws_role_web_identity_token_file: String::new(),
        aws_role_arn: String::new(),
        aws_role_session_name: String::new(),
        aws_role_duration_seconds: 0,
    }
}

fn external_tier_alias_from_compatible_payload(
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
    prefix: String,
    region: String,
) -> ExternalTierCompatible {
    ExternalTierCompatible {
        endpoint,
        access_key,
        secret_key,
        bucket,
        prefix,
        region,
    }
}

fn to_external_tier_config(name: &str, tier: &TierConfig) -> io::Result<ExternalTierConfig> {
    let mut out = ExternalTierConfig {
        version: if tier.version.is_empty() {
            "v1".to_string()
        } else {
            tier.version.clone()
        },
        name: if tier.name.is_empty() {
            name.to_string()
        } else {
            tier.name.clone()
        },
        ..Default::default()
    };

    match tier.tier_type {
        TierType::S3 => {
            let s3 = tier
                .s3
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing s3 backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.s3 = Some(external_tier_s3_from_internal(s3));
        }
        TierType::Wasabi => {
            let backend = tier
                .wasabi
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing Wasabi backend payload"))?;
            out.version = EXTERNAL_TIER_VERSION_WASABI_V1.to_string();
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.tier_type_hint = tier_hint_for_type(tier.tier_type.clone()).map(ToString::to_string);
            out.s3 = Some(external_tier_s3_from_compatible_payload(
                {
                    backend.canonical_endpoint()?;
                    EXTERNAL_TIER_WASABI_ENDPOINT_SENTINEL.to_string()
                },
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::Azure => {
            let az = tier
                .azure
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing azure backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_AZURE;
            out.azure = Some(ExternalTierAzure {
                endpoint: az.endpoint.clone(),
                account_name: az.access_key.clone(),
                account_key: az.secret_key.clone(),
                bucket: az.bucket.clone(),
                prefix: az.prefix.clone(),
                region: az.region.clone(),
                storage_class: az.storage_class.clone(),
                sp_auth: ExternalServicePrincipalAuth {
                    tenant_id: az.sp_auth.tenant_id.clone(),
                    client_id: az.sp_auth.client_id.clone(),
                    client_secret: az.sp_auth.client_secret.clone(),
                },
            });
        }
        TierType::GCS => {
            let gcs = tier
                .gcs
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing gcs backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_GCS;
            out.gcs = Some(ExternalTierGcs {
                endpoint: gcs.endpoint.clone(),
                creds: gcs.creds.clone(),
                bucket: gcs.bucket.clone(),
                prefix: gcs.prefix.clone(),
                region: gcs.region.clone(),
                storage_class: gcs.storage_class.clone(),
            });
        }
        TierType::MinIO => {
            let backend = tier
                .minio
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_MINIO;
            out.compatible_backend = Some(external_tier_alias_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::RustFS => {
            let backend = tier
                .rustfs
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.tier_type_hint = tier_hint_for_type(tier.tier_type.clone()).map(ToString::to_string);
            out.s3 = Some(external_tier_s3_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::Aliyun => {
            let backend = tier
                .aliyun
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.tier_type_hint = tier_hint_for_type(tier.tier_type.clone()).map(ToString::to_string);
            out.s3 = Some(external_tier_s3_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::Tencent => {
            let backend = tier
                .tencent
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.tier_type_hint = tier_hint_for_type(tier.tier_type.clone()).map(ToString::to_string);
            out.s3 = Some(external_tier_s3_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::Huaweicloud => {
            let backend = tier
                .huaweicloud
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.tier_type_hint = tier_hint_for_type(tier.tier_type.clone()).map(ToString::to_string);
            out.s3 = Some(external_tier_s3_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::R2 => {
            let backend = tier
                .r2
                .as_ref()
                .ok_or_else(|| io::Error::other("tier config missing compatible backend payload"))?;
            out.tier_type = EXTERNAL_TIER_TYPE_S3;
            out.tier_type_hint = tier_hint_for_type(tier.tier_type.clone()).map(ToString::to_string);
            out.s3 = Some(external_tier_s3_from_compatible_payload(
                backend.endpoint.clone(),
                backend.access_key.clone(),
                backend.secret_key.clone(),
                backend.bucket.clone(),
                backend.prefix.clone(),
                backend.region.clone(),
            ));
        }
        TierType::Unsupported => {
            out.tier_type = EXTERNAL_TIER_TYPE_UNSUPPORTED;
        }
    }
    Ok(out)
}

fn decode_legacy_s3_like(name: &str, ext: &ExternalTierConfig) -> io::Result<ExternalTierS3> {
    if let Some(s3) = ext.s3.as_ref() {
        return Ok(s3.clone());
    }
    if let Some(m) = ext.compatible_backend.as_ref() {
        return Ok(external_tier_s3_from_compatible_payload(
            m.endpoint.clone(),
            m.access_key.clone(),
            m.secret_key.clone(),
            m.bucket.clone(),
            m.prefix.clone(),
            m.region.clone(),
        ));
    }
    Err(io::Error::other(format!("tier config '{name}' missing compatible backend payload")))
}

fn from_external_tier_config(name: String, ext: ExternalTierConfig) -> io::Result<TierConfig> {
    let mut cfg = TierConfig {
        version: if ext.version.is_empty() {
            "v1".to_string()
        } else {
            ext.version.clone()
        },
        name: if ext.name.is_empty() { name } else { ext.name.clone() },
        ..Default::default()
    };

    let wasabi_hint = ext.tier_type_hint.as_deref() == Some("wasabi");
    let wasabi_version = ext.version == EXTERNAL_TIER_VERSION_WASABI_V1;
    if wasabi_hint && !wasabi_version {
        return Err(io::Error::other(format!(
            "tier config '{}' has inconsistent Wasabi type discriminators",
            cfg.name
        )));
    }
    if (wasabi_hint || wasabi_version) && ext.tier_type != EXTERNAL_TIER_TYPE_S3 {
        return Err(io::Error::other(format!(
            "tier config '{}' has inconsistent Wasabi type discriminators",
            cfg.name
        )));
    }
    if wasabi_version && ext.tier_type_hint.as_deref().is_some_and(|hint| hint != "wasabi") {
        return Err(io::Error::other(format!(
            "tier config '{}' has inconsistent Wasabi type discriminators",
            cfg.name
        )));
    }
    let tier_type = if wasabi_version {
        TierType::Wasabi
    } else {
        tier_type_from_hint(ext.tier_type_hint.as_deref()).unwrap_or(match ext.tier_type {
            EXTERNAL_TIER_TYPE_S3 => TierType::S3,
            EXTERNAL_TIER_TYPE_AZURE => TierType::Azure,
            EXTERNAL_TIER_TYPE_GCS => TierType::GCS,
            EXTERNAL_TIER_TYPE_MINIO => TierType::MinIO,
            _ => TierType::Unsupported,
        })
    };

    cfg.tier_type = tier_type.clone();

    match tier_type {
        TierType::S3 => {
            let s3 = ext
                .s3
                .as_ref()
                .ok_or_else(|| io::Error::other(format!("tier config '{}' missing s3 backend payload", cfg.name)))?;
            cfg.s3 = Some(crate::services::tier::tier_config::TierS3 {
                name: cfg.name.clone(),
                endpoint: s3.endpoint.clone(),
                access_key: s3.access_key.clone(),
                secret_key: s3.secret_key.clone(),
                bucket: s3.bucket.clone(),
                prefix: s3.prefix.clone(),
                region: s3.region.clone(),
                storage_class: s3.storage_class.clone(),
                aws_role: s3.aws_role,
                aws_role_web_identity_token_file: s3.aws_role_web_identity_token_file.clone(),
                aws_role_arn: s3.aws_role_arn.clone(),
                aws_role_session_name: s3.aws_role_session_name.clone(),
                aws_role_duration_seconds: s3.aws_role_duration_seconds,
            });
        }
        TierType::Wasabi => {
            let s3 = ext
                .s3
                .as_ref()
                .ok_or_else(|| io::Error::other(format!("tier config '{}' missing Wasabi S3 payload", cfg.name)))?;
            if !s3.storage_class.is_empty()
                || s3.aws_role
                || !s3.aws_role_web_identity_token_file.is_empty()
                || !s3.aws_role_arn.is_empty()
                || !s3.aws_role_session_name.is_empty()
                || s3.aws_role_duration_seconds != 0
            {
                return Err(io::Error::other(format!(
                    "tier config '{}' has unsupported Wasabi S3 credential fields",
                    cfg.name
                )));
            }
            if s3.endpoint != EXTERNAL_TIER_WASABI_ENDPOINT_SENTINEL {
                return Err(io::Error::other(format!(
                    "tier config '{}' has an invalid Wasabi compatibility endpoint",
                    cfg.name
                )));
            }
            let mut wasabi = TierWasabi {
                name: cfg.name.clone(),
                endpoint: String::new(),
                access_key: s3.access_key.clone(),
                secret_key: s3.secret_key.clone(),
                bucket: s3.bucket.clone(),
                prefix: s3.prefix.clone(),
                region: s3.region.clone(),
            };
            wasabi.normalize_endpoint()?;
            cfg.wasabi = Some(wasabi);
        }
        TierType::Azure => {
            let az = ext
                .azure
                .as_ref()
                .ok_or_else(|| io::Error::other(format!("tier config '{}' missing azure backend payload", cfg.name)))?;
            cfg.azure = Some(crate::services::tier::tier_config::TierAzure {
                name: cfg.name.clone(),
                endpoint: az.endpoint.clone(),
                access_key: az.account_name.clone(),
                secret_key: az.account_key.clone(),
                bucket: az.bucket.clone(),
                prefix: az.prefix.clone(),
                region: az.region.clone(),
                storage_class: az.storage_class.clone(),
                sp_auth: crate::services::tier::tier_config::ServicePrincipalAuth {
                    tenant_id: az.sp_auth.tenant_id.clone(),
                    client_id: az.sp_auth.client_id.clone(),
                    client_secret: az.sp_auth.client_secret.clone(),
                },
            });
        }
        TierType::GCS => {
            let gcs = ext
                .gcs
                .as_ref()
                .ok_or_else(|| io::Error::other(format!("tier config '{}' missing gcs backend payload", cfg.name)))?;
            cfg.gcs = Some(crate::services::tier::tier_config::TierGCS {
                name: cfg.name.clone(),
                endpoint: gcs.endpoint.clone(),
                creds: decode_external_gcs_credentials(&gcs.creds)?,
                bucket: gcs.bucket.clone(),
                prefix: gcs.prefix.clone(),
                region: gcs.region.clone(),
                storage_class: gcs.storage_class.clone(),
            });
        }
        TierType::MinIO => {
            let m = ext
                .compatible_backend
                .as_ref()
                .ok_or_else(|| io::Error::other(format!("tier config '{}' missing compatible backend payload", cfg.name)))?;
            cfg.minio = Some(crate::services::tier::tier_config::TierMinIO {
                name: cfg.name.clone(),
                endpoint: m.endpoint.clone(),
                access_key: m.access_key.clone(),
                secret_key: m.secret_key.clone(),
                bucket: m.bucket.clone(),
                prefix: m.prefix.clone(),
                region: m.region.clone(),
            });
        }
        TierType::RustFS => {
            let m = decode_legacy_s3_like(&cfg.name, &ext)?;
            cfg.rustfs = Some(crate::services::tier::tier_config::TierRustFS {
                name: cfg.name.clone(),
                endpoint: m.endpoint,
                access_key: m.access_key,
                secret_key: m.secret_key,
                bucket: m.bucket,
                prefix: m.prefix,
                region: m.region,
                storage_class: m.storage_class,
            });
        }
        TierType::Aliyun => {
            let m = decode_legacy_s3_like(&cfg.name, &ext)?;
            cfg.aliyun = Some(crate::services::tier::tier_config::TierAliyun {
                name: cfg.name.clone(),
                endpoint: m.endpoint,
                access_key: m.access_key,
                secret_key: m.secret_key,
                bucket: m.bucket,
                prefix: m.prefix,
                region: m.region,
            });
        }
        TierType::Tencent => {
            let m = decode_legacy_s3_like(&cfg.name, &ext)?;
            cfg.tencent = Some(crate::services::tier::tier_config::TierTencent {
                name: cfg.name.clone(),
                endpoint: m.endpoint,
                access_key: m.access_key,
                secret_key: m.secret_key,
                bucket: m.bucket,
                prefix: m.prefix,
                region: m.region,
            });
        }
        TierType::Huaweicloud => {
            let m = decode_legacy_s3_like(&cfg.name, &ext)?;
            cfg.huaweicloud = Some(crate::services::tier::tier_config::TierHuaweicloud {
                name: cfg.name.clone(),
                endpoint: m.endpoint,
                access_key: m.access_key,
                secret_key: m.secret_key,
                bucket: m.bucket,
                prefix: m.prefix,
                region: m.region,
            });
        }
        TierType::R2 => {
            let m = decode_legacy_s3_like(&cfg.name, &ext)?;
            cfg.r2 = Some(crate::services::tier::tier_config::TierR2 {
                name: cfg.name.clone(),
                endpoint: m.endpoint,
                access_key: m.access_key,
                secret_key: m.secret_key,
                bucket: m.bucket,
                prefix: m.prefix,
                region: m.region,
            });
        }
        TierType::Unsupported => {}
    }
    Ok(cfg)
}

fn encode_external_tiering_config_blob(cfg: &TierConfigMgr) -> io::Result<Bytes> {
    let mut tiers = BTreeMap::new();
    for (name, tier_cfg) in &cfg.tiers {
        tiers.insert(name.clone(), to_external_tier_config(name, tier_cfg)?);
    }
    let payload = rmp_serde::to_vec(&ExternalTierConfigMgr { tiers })
        .map_err(|err| io::Error::other(format!("serialize tier config payload failed: {err}")))?;
    let mut data = Vec::with_capacity(4 + payload.len());
    let mut format = [0u8; 2];
    LittleEndian::write_u16(&mut format, TIER_CONFIG_FORMAT);
    data.extend_from_slice(&format);
    let mut version = [0u8; 2];
    LittleEndian::write_u16(&mut version, TIER_CONFIG_VERSION);
    data.extend_from_slice(&version);
    data.extend_from_slice(&payload);
    Ok(Bytes::from(data))
}

fn decode_external_tiering_config_blob(data: &[u8]) -> io::Result<TierConfigMgr> {
    if data.len() <= 4 {
        return Err(io::Error::other("tierConfigInit: no data"));
    }
    let format = LittleEndian::read_u16(&data[0..2]);
    if format != TIER_CONFIG_FORMAT {
        return Err(io::Error::other(format!("tierConfigInit: unknown format: {format}")));
    }
    let version = LittleEndian::read_u16(&data[2..4]);
    if version != TIER_CONFIG_V1 && version != TIER_CONFIG_VERSION {
        return Err(io::Error::other(format!("tierConfigInit: unknown version: {version}")));
    }

    let external: ExternalTierConfigMgr =
        rmp_serde::from_slice(&data[4..]).map_err(|err| io::Error::other(format!("decode tier config payload failed: {err}")))?;
    let mut tiers = HashMap::with_capacity(external.tiers.len());
    for (name, ext_cfg) in external.tiers {
        tiers.insert(name.clone(), from_external_tier_config(name, ext_cfg)?);
    }
    Ok(TierConfigMgr {
        driver_cache: HashMap::new(),
        tiers,
        last_refreshed_at: OffsetDateTime::now_utc(),
    })
}

fn decode_tiering_config_blob(data: &[u8]) -> io::Result<TierConfigMgr> {
    if let Ok(cfg) = TierConfigMgr::unmarshal(data) {
        return Ok(cfg);
    }
    decode_external_tiering_config_blob(data)
}

impl TierConfigMgr {
    pub fn new() -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            driver_cache: HashMap::new(),
            tiers: HashMap::new(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        }))
    }

    pub fn unmarshal(data: &[u8]) -> std::result::Result<TierConfigMgr, std::io::Error> {
        let mut cfg: TierConfigMgr = serde_json::from_slice(data)?;
        for (name, tier) in &mut cfg.tiers {
            if matches!(&tier.tier_type, TierType::Wasabi) {
                let wasabi = tier
                    .wasabi
                    .as_mut()
                    .ok_or_else(|| io::Error::other(format!("tier config '{name}' missing Wasabi backend payload")))?;
                if wasabi.endpoint.is_empty() {
                    return Err(io::Error::other(format!("tier config '{name}' missing Wasabi endpoint")));
                }
                wasabi.normalize_endpoint()?;
            }
        }
        Ok(cfg)
    }

    pub fn marshal(&self) -> std::result::Result<Bytes, std::io::Error> {
        let data = serde_json::to_vec(&self)?;
        let mut data = Bytes::from(data);

        Ok(data)
    }

    pub fn refreshed_at(&self) -> OffsetDateTime {
        self.last_refreshed_at
    }

    pub fn is_tier_valid(&self, tier_name: &str) -> bool {
        let (_, valid) = self.is_tier_name_in_use(tier_name);
        valid
    }

    pub fn is_tier_name_in_use(&self, tier_name: &str) -> (TierType, bool) {
        if let Some(t) = self.tiers.get(tier_name) {
            return (t.tier_type.clone(), true);
        }
        (TierType::Unsupported, false)
    }

    pub async fn add(&mut self, tier_config: TierConfig, force: bool) -> std::result::Result<(), AdminError> {
        self.add_with_deadline(tier_config, force, None).await
    }

    async fn add_with_deadline(
        &mut self,
        mut tier_config: TierConfig,
        force: bool,
        deadline: Option<Instant>,
    ) -> std::result::Result<(), AdminError> {
        normalize_s3_gcs_add_tier_name(&mut tier_config)?;
        self.ensure_generation_is_idle(&tier_config.name)?;
        let tier_name = tier_config.name.clone();
        if tier_name != tier_name.to_uppercase() {
            return Err(ERR_TIER_NAME_NOT_UPPERCASE.clone());
        }

        // Reject AWS/MinIO reserved storage-class names as remote-tier names.
        // MinIO reserves STANDARD and REDUCED_REDUNDANCY (RRS); a tier must not
        // shadow them. The comparison is case-insensitive to match MinIO parity
        // and to stay robust regardless of the uppercase gate above.
        if tier_name.eq_ignore_ascii_case(crate::config::storageclass::STANDARD)
            || tier_name.eq_ignore_ascii_case(crate::config::storageclass::RRS)
        {
            return Err(ERR_TIER_RESERVED_NAME.clone());
        }

        let (_, b) = self.is_tier_name_in_use(&tier_name);
        if b {
            return Err(ERR_TIER_ALREADY_EXISTS.clone());
        }

        if matches!(&tier_config.tier_type, TierType::Wasabi)
            && let Some(wasabi) = tier_config.wasabi.as_mut()
        {
            wasabi.normalize_endpoint().map_err(|source| {
                let mut err = ERR_TIER_INVALID_CONFIG.clone();
                err.message = source.to_string();
                err
            })?;
        }

        if matches!(&tier_config.tier_type, TierType::GCS)
            && let Some(gcs) = tier_config.gcs.as_mut()
        {
            gcs.creds = decode_external_gcs_credentials(&gcs.creds).map_err(|source| tier_invalid_config(source.to_string()))?;
        }

        validate_tier_config_credentials(&tier_config)?;
        let d = match deadline {
            Some(deadline) => build_warm_backend_with_deadline(&tier_config, true, Some(deadline)).await?,
            None => build_warm_backend(&tier_config, true).await?,
        };

        if !force {
            let in_use = match deadline {
                Some(deadline) => timeout_at(deadline, d.in_use()).await,
                None => tokio::time::timeout(WARM_BACKEND_PROBE_TIMEOUT, d.in_use()).await,
            }
            .map_err(|_| {
                let mut err = ERR_TIER_BACKEND_IN_USE.clone();
                err.message = "Timed out checking whether the remote tier is in use".to_string();
                err
            })?;
            match in_use {
                Ok(b) => {
                    if b {
                        return Err(ERR_TIER_BACKEND_IN_USE.clone());
                    }
                }
                Err(err) => {
                    warn!("tier add failed, err: {:?}", err);
                    if err.to_string().contains("connect") {
                        return Err(ERR_TIER_CONNECT_ERR.clone());
                    } else if err.to_string().contains("authorization") {
                        return Err(ERR_TIER_INVALID_CREDENTIALS.clone());
                    } else if err.to_string().contains("bucket") {
                        return Err(ERR_TIER_BUCKET_NOT_FOUND.clone());
                    }
                    let mut e = ERR_TIER_PERM_ERR.clone();
                    e.message.push('.');
                    e.message.push_str(&err.to_string());
                    return Err(e);
                }
            }
        }

        self.tiers.insert(tier_name.clone(), tier_config);
        if let Err(err) = self.replace_driver(&tier_name, d) {
            self.tiers.remove(&tier_name);
            return Err(err);
        }
        Ok(())
    }

    pub async fn remove(&mut self, tier_name: &str, force: bool) -> std::result::Result<(), AdminError> {
        self.ensure_generation_is_idle(tier_name)?;
        let driver = match self.get_driver(tier_name).await {
            Ok(driver) => driver,
            Err(err) if err.code == ERR_TIER_NOT_FOUND.code => return Ok(()),
            Err(err) => return Err(err),
        };
        if !force {
            match driver.in_use().await {
                Err(err) => {
                    let mut e = ERR_TIER_PERM_ERR.clone();
                    e.message.push('.');
                    e.message.push_str(&err.to_string());
                    return Err(e);
                }
                Ok(in_use) if in_use => {
                    return Err(ERR_TIER_BACKEND_NOT_EMPTY.clone());
                }
                _ => {}
            }
        }
        self.tiers.remove(tier_name);
        self.revoke_driver(tier_name);
        Ok(())
    }

    pub async fn verify(&mut self, tier_name: &str) -> std::result::Result<(), std::io::Error> {
        let driver = self.get_driver(tier_name).await.map_err(std::io::Error::other)?;
        check_warm_backend(Some(driver)).await.map_err(std::io::Error::other)
    }

    pub fn empty(&self) -> bool {
        self.tiers.is_empty()
    }

    pub fn tier_type(&self, tier_name: &str) -> String {
        if let Some(cfg) = self.tiers.get(tier_name) {
            cfg.tier_type.as_lowercase()
        } else {
            "internal".to_string()
        }
    }

    pub fn list_tiers(&self) -> Vec<TierConfig> {
        let mut tier_cfgs = Vec::<TierConfig>::new();
        for tier in self.tiers.values() {
            let tier = tier.redacted();
            tier_cfgs.push(tier);
        }
        tier_cfgs
    }

    pub fn get(&self, tier_name: &str) -> Option<TierConfig> {
        for (tier_name2, tier) in self.tiers.iter() {
            if tier_name == tier_name2 {
                return Some(tier.redacted());
            }
        }
        None
    }

    pub async fn edit(&mut self, tier_name: &str, creds: TierCreds) -> std::result::Result<(), AdminError> {
        self.edit_with_deadline(tier_name, creds, None).await
    }

    async fn edit_with_deadline(
        &mut self,
        tier_name: &str,
        creds: TierCreds,
        deadline: Option<Instant>,
    ) -> std::result::Result<(), AdminError> {
        self.ensure_generation_is_idle(tier_name)?;
        let (tier_type, exists) = self.is_tier_name_in_use(tier_name);
        if !exists {
            return Err(ERR_TIER_NOT_FOUND.clone());
        }
        if !creds.azure_service_principal.is_empty() {
            return Err(tier_invalid_config(
                "Azure service principal credentials are not supported for remote tier edits",
            ));
        }

        let mut tier_config = self.tiers[tier_name].clone_with_credentials();
        match tier_type {
            TierType::S3 => {
                if let Some(s3) = tier_config.s3.as_mut() {
                    merge_static_tier_credentials(&mut s3.access_key, &mut s3.secret_key, &creds)?;
                }
            }
            TierType::Wasabi => {
                if let Some(wasabi) = tier_config.wasabi.as_mut() {
                    merge_static_tier_credentials(&mut wasabi.access_key, &mut wasabi.secret_key, &creds)?;
                }
            }
            TierType::RustFS => {
                if let Some(rustfs) = tier_config.rustfs.as_mut() {
                    merge_static_tier_credentials(&mut rustfs.access_key, &mut rustfs.secret_key, &creds)?;
                }
            }
            TierType::MinIO => {
                if let Some(compatible_backend) = tier_config.minio.as_mut() {
                    merge_static_tier_credentials(
                        &mut compatible_backend.access_key,
                        &mut compatible_backend.secret_key,
                        &creds,
                    )?;
                }
            }
            TierType::Aliyun => {
                if let Some(aliyun) = tier_config.aliyun.as_mut() {
                    merge_static_tier_credentials(&mut aliyun.access_key, &mut aliyun.secret_key, &creds)?;
                }
            }
            TierType::Tencent => {
                if let Some(tencent) = tier_config.tencent.as_mut() {
                    merge_static_tier_credentials(&mut tencent.access_key, &mut tencent.secret_key, &creds)?;
                }
            }
            TierType::Huaweicloud => {
                if let Some(huaweicloud) = tier_config.huaweicloud.as_mut() {
                    merge_static_tier_credentials(&mut huaweicloud.access_key, &mut huaweicloud.secret_key, &creds)?;
                }
            }
            TierType::Azure => {
                if let Some(azure) = tier_config.azure.as_mut() {
                    merge_static_tier_credentials(&mut azure.access_key, &mut azure.secret_key, &creds)?;
                }
            }
            TierType::GCS => {
                if let Some(gcs) = tier_config.gcs.as_mut() {
                    if tier_creds_request_aws_role(&creds) {
                        return Err(reject_unsupported_aws_role());
                    }
                    if !creds.access_key.is_empty() || !creds.secret_key.is_empty() {
                        return Err(tier_invalid_config("Static access and secret keys cannot be used with a GCS tier"));
                    }
                    if !creds.creds_json.is_empty() {
                        let credentials = std::str::from_utf8(&creds.creds_json)
                            .map_err(|_| tier_invalid_config("GCS credentials must be UTF-8 service account JSON"))?;
                        validate_gcs_credentials_json(credentials)?;
                        gcs.creds = credentials.to_string();
                    }
                }
            }
            TierType::R2 => {
                if let Some(r2) = tier_config.r2.as_mut() {
                    merge_static_tier_credentials(&mut r2.access_key, &mut r2.secret_key, &creds)?;
                }
            }
            _ => (),
        }

        validate_tier_config_credentials(&tier_config)?;
        let d = match deadline {
            Some(deadline) => build_warm_backend_with_deadline(&tier_config, true, Some(deadline)).await?,
            None => build_warm_backend(&tier_config, true).await?,
        };
        self.revoke_driver(tier_name);
        self.tiers.insert(tier_name.to_string(), tier_config);
        self.replace_driver(tier_name, d)?;
        Ok(())
    }

    pub async fn get_driver<'a>(&'a mut self, tier_name: &str) -> std::result::Result<&'a WarmBackendImpl, AdminError> {
        if registered_tier_driver_runtime(self).is_some_and(|runtime| runtime_blocks_tier(&lock_unpoisoned(&runtime), tier_name))
        {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = TIER_MUTATION_BLOCK_MESSAGE.to_string();
            return Err(err);
        }
        // Return cached driver if present
        if self.driver_cache.contains_key(tier_name) {
            return Ok(self.driver_cache.get(tier_name).expect("Driver not found in cache"));
        }

        // Get tier configuration and create new driver
        let tier_config = self.tiers.get(tier_name).ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?;

        let driver = construct_warm_backend(tier_config).await?;

        self.replace_driver(tier_name, driver)?;
        Ok(self
            .driver_cache
            .get(tier_name)
            .expect("Driver not found in cache after insertion"))
    }

    pub(crate) async fn acquire_operation_lease(
        handle: &Arc<RwLock<Self>>,
        tier_name: &str,
    ) -> std::result::Result<TierOperationLease, AdminError> {
        {
            let manager = handle.read().await;
            let runtime = tier_driver_runtime(handle, &manager);
            let runtime_guard = lock_unpoisoned(&runtime);
            if runtime_blocks_tier(&runtime_guard, tier_name) {
                let mut err = ERR_TIER_INVALID_CONFIG.clone();
                err.message = TIER_MUTATION_BLOCK_MESSAGE.to_string();
                return Err(err);
            }
            if let Some(generation) = runtime_guard
                .generations
                .get(tier_name)
                .filter(|generation| generation.accepting.load(Ordering::Acquire))
                .cloned()
            {
                let lease = TierOperationLease::try_new(generation, runtime.clone());
                drop(runtime_guard);
                return lease;
            }
        }

        let (config, config_fingerprint) = {
            let mut manager = handle.write().await;
            let runtime = tier_driver_runtime(handle, &manager);
            if runtime_blocks_tier(&lock_unpoisoned(&runtime), tier_name) {
                let mut err = ERR_TIER_INVALID_CONFIG.clone();
                err.message = TIER_MUTATION_BLOCK_MESSAGE.to_string();
                return Err(err);
            }
            if let Some(driver) = manager.driver_cache.remove(tier_name) {
                manager.replace_driver(tier_name, driver)?;
                let runtime_guard = lock_unpoisoned(&runtime);
                let generation = runtime_guard
                    .generations
                    .get(tier_name)
                    .filter(|generation| generation.accepting.load(Ordering::Acquire))
                    .ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?
                    .clone();
                let lease = TierOperationLease::try_new(generation, runtime.clone());
                drop(runtime_guard);
                return lease;
            }
            let config = manager
                .tiers
                .get(tier_name)
                .ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?
                .clone_with_credentials();
            let fingerprint = tier_config_fingerprint(tier_name, &config).map_err(|err| {
                let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                admin_err.message = err.to_string();
                admin_err
            })?;
            (config, fingerprint)
        };

        let driver = build_warm_backend(&config, false).await?;
        let mut manager = handle.write().await;
        let runtime = tier_driver_runtime(handle, &manager);
        let runtime_guard = lock_unpoisoned(&runtime);
        if runtime_blocks_tier(&runtime_guard, tier_name) {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = TIER_MUTATION_BLOCK_MESSAGE.to_string();
            return Err(err);
        }
        if let Some(generation) = runtime_guard
            .generations
            .get(tier_name)
            .filter(|generation| {
                generation.config_fingerprint == config_fingerprint && generation.accepting.load(Ordering::Acquire)
            })
            .cloned()
        {
            let lease = TierOperationLease::try_new(generation, runtime.clone());
            drop(runtime_guard);
            return lease;
        }
        drop(runtime_guard);
        let current = manager.tiers.get(tier_name).ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?;
        if tier_config_fingerprint(tier_name, current).map_err(|err| {
            let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
            admin_err.message = err.to_string();
            admin_err
        })? != config_fingerprint
        {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier configuration changed while its driver was being prepared".to_string();
            return Err(err);
        }
        manager.replace_driver(tier_name, driver)?;
        let runtime_guard = lock_unpoisoned(&runtime);
        let generation = runtime_guard
            .generations
            .get(tier_name)
            .filter(|generation| generation.accepting.load(Ordering::Acquire))
            .ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?
            .clone();
        let lease = TierOperationLease::try_new(generation, runtime.clone());
        drop(runtime_guard);
        lease
    }

    pub(crate) fn operation_lease_blocked_by_mutation(err: &AdminError) -> bool {
        err.message == TIER_MUTATION_BLOCK_MESSAGE
    }

    async fn admin_update_lock(handle: &Arc<RwLock<Self>>) -> tokio::sync::OwnedMutexGuard<()> {
        let update_lock = {
            let manager = handle.read().await;
            let runtime = tier_driver_runtime(handle, &manager);
            lock_unpoisoned(&runtime).admin_updates.clone()
        };
        update_lock.lock_owned().await
    }

    async fn mutation_refresh_notifier(handle: &Arc<RwLock<Self>>) -> Arc<Notify> {
        let manager = handle.read().await;
        let runtime = tier_driver_runtime(handle, &manager);
        lock_unpoisoned(&runtime).mutation_refresh.clone()
    }

    pub(crate) async fn request_committed_mutation_refresh(handle: &Arc<RwLock<Self>>) {
        Self::mutation_refresh_notifier(handle).await.notify_one();
    }

    async fn mutation_block_allowance_for(
        handle: &Arc<RwLock<Self>>,
        mutation_id: uuid::Uuid,
    ) -> std::result::Result<MutationBlockAllowance, AdminError> {
        let manager = handle.read().await;
        let runtime = tier_driver_runtime(handle, &manager);
        let runtime = lock_unpoisoned(&runtime);
        let prepared = runtime
            .prepared_mutation_blocks
            .values()
            .any(|blocked_mutation_id| *blocked_mutation_id == mutation_id);
        let committed = runtime
            .committed_mutation_blocks
            .values()
            .any(|mutation_ids| mutation_ids.contains(&mutation_id));
        if !prepared && !committed {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier mutation fence was not installed".to_string();
            return Err(err);
        }
        Ok(MutationBlockAllowance {
            mutation_ids: HashSet::from([mutation_id]),
            revision: runtime.mutation_blocks_revision,
        })
    }

    async fn has_committed_mutation_block(handle: &Arc<RwLock<Self>>) -> bool {
        let manager = handle.read().await;
        registered_tier_driver_runtime(&manager)
            .is_some_and(|runtime| !lock_unpoisoned(&runtime).committed_mutation_blocks.is_empty())
    }

    async fn has_retryable_terminal_mutation_cleanup<S>(api: Arc<S>) -> io::Result<bool>
    where
        S: EcstoreObjectIO + ListOperations<Error = Error, ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>>,
    {
        let now = i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap_or(i64::MAX);
        let peer_cleanup = Self::load_tier_mutation_intents(api.clone())
            .await?
            .into_iter()
            .any(|intent| {
                intent.state != TierMutationIntentState::Prepared && Self::tier_mutation_tombstone_retention_elapsed(&intent, now)
            });
        if peer_cleanup {
            return Ok(true);
        }
        Ok(Self::load_coordinator_mutation_intents(api)
            .await?
            .into_iter()
            .any(|intent| intent.state != TierMutationIntentState::Prepared))
    }

    fn terminal_mutation_cleanup_requires_retry(result: io::Result<bool>) -> bool {
        // A failed durable-state read is Unknown, not proof that no terminal
        // cleanup remains. Keep the bounded-backoff retry loop alive until a
        // later authoritative read can classify the state.
        result.unwrap_or(true)
    }

    async fn acquire_tier_config_write_lock<S>(
        api: Arc<S>,
    ) -> std::result::Result<rustfs_lock::NamespaceLockGuard, TierConfigUpdateError>
    where
        S: NamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper> + 'static,
    {
        let config_file = tier_config_lock_path();
        let ns_lock = api
            .new_ns_lock(RUSTFS_META_BUCKET, &config_file)
            .await
            .map_err(|err| TierConfigUpdateError::Load(io::Error::other(err)))?;
        ns_lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(|err| TierConfigUpdateError::Load(io::Error::other(err)))
    }

    async fn update_candidate_with_config_lock<S>(
        handle: &Arc<RwLock<Self>>,
        api: Arc<S>,
        mut mutation: TierCandidateMutation,
    ) -> std::result::Result<(), TierConfigUpdateError>
    where
        S: TierReferenceProofStore + NamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper> + 'static,
    {
        mutation.normalize_add_tier_name().map_err(TierConfigUpdateError::Mutation)?;
        let initial_config_lock = Self::acquire_tier_config_write_lock(api.clone()).await?;
        Self::reject_pending_mutation_recovery_before_update(handle, api.clone()).await?;
        let (candidate, version) = load_tier_config_for_update(api.clone())
            .await
            .map_err(TierConfigUpdateError::Load)?;
        drop(initial_config_lock);

        let prepared = Self::prevalidate_candidate_owned(candidate, version, mutation).await?;

        let config_lock = Self::acquire_tier_config_write_lock(api.clone()).await?;
        Self::reject_pending_mutation_recovery_before_update(handle, api.clone()).await?;
        let update = Self::admin_update_lock(handle).await;
        let (candidate, version) = load_tier_config_for_update(api.clone())
            .await
            .map_err(TierConfigUpdateError::Load)?;
        if version != prepared.version {
            return Err(TierConfigUpdateError::Load(io::Error::other(
                "tier configuration changed while the remote backend mutation was being validated",
            )));
        }
        Self::update_candidate_owned(
            handle,
            api,
            candidate,
            version,
            TierCandidateMutation::Prevalidated(Box::new(prepared)),
            update,
            Some(config_lock),
        )
        .await
    }

    async fn prevalidate_candidate_owned(
        candidate: TierConfigMgr,
        version: Option<String>,
        mutation: TierCandidateMutation,
    ) -> std::result::Result<PrevalidatedTierCandidateMutation, TierConfigUpdateError> {
        #[cfg(test)]
        let test_driver_factory = TIER_DRIVER_TEST_FACTORY.try_with(|factory| factory.clone()).ok();
        tokio::spawn(async move {
            let validation = prevalidate_tier_candidate_mutation(candidate, version, mutation);
            #[cfg(test)]
            if let Some(factory) = test_driver_factory {
                return TIER_DRIVER_TEST_FACTORY.scope(factory, validation).await;
            }
            validation.await
        })
        .await
        .map_err(|err| {
            let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
            admin_err.message = format!("Remote tier backend validation task failed: {err}");
            TierConfigUpdateError::Mutation(admin_err)
        })?
    }

    async fn revalidate_prepared_candidate_before_commit<S>(
        api: Arc<S>,
        version: Option<&str>,
        candidate_digest: TierMutationDigest,
        coordinator_intent: Option<&TierMutationIntent>,
    ) -> io::Result<()>
    where
        S: TierReferenceProofStore,
    {
        let (_, current_version) = load_tier_config_for_update(api.clone()).await?;
        if current_version.as_deref() != version {
            return Err(io::Error::other(
                "tier configuration changed while the prepared mutation reference proof was running",
            ));
        }
        let Some(expected) = coordinator_intent else {
            return Ok(());
        };
        if expected.candidate_digest != candidate_digest {
            return Err(io::Error::other("prepared tier mutation candidate digest changed before commit"));
        }
        let (persisted, _) = load_tier_coordinator_mutation_intent_record_with_etag(api, expected.mutation_id)
            .await
            .map_err(io::Error::other)?;
        if persisted.state != TierMutationIntentState::Prepared || !persisted.same_identity_as(expected) {
            return Err(io::Error::other(
                "prepared tier mutation intent changed while the reference proof was running",
            ));
        }
        let now = i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap_or(i64::MAX);
        if persisted.expires_at_unix_nanos <= now {
            return Err(io::Error::other("prepared tier mutation intent expired before config commit"));
        }
        Ok(())
    }

    async fn reject_pending_mutation_recovery_before_update<S>(
        handle: &Arc<RwLock<Self>>,
        api: Arc<S>,
    ) -> std::result::Result<(), TierConfigUpdateError>
    where
        S: TierReferenceProofStore + 'static,
    {
        let mut snapshot = Self::load_and_reconcile_mutation_recovery_snapshot(handle, api.clone(), true)
            .await
            .map_err(TierConfigUpdateError::Load)?;
        Self::recover_prepared_coordinator_mutation_intents(api.clone(), &mut snapshot.coordinator_intents)
            .await
            .map_err(TierConfigUpdateError::Load)?;

        let snapshot = Self::load_and_reconcile_mutation_recovery_snapshot(handle, api, true)
            .await
            .map_err(TierConfigUpdateError::Load)?;
        if snapshot.is_quiescent() {
            return Ok(());
        }

        Self::request_committed_mutation_refresh(handle).await;
        let mut err = ERR_TIER_BACKEND_IN_USE.clone();
        err.message = "Remote tier mutation recovery must finish before applying a new tier configuration update".to_string();
        Err(TierConfigUpdateError::Publish(err))
    }

    #[cfg(test)]
    pub(crate) async fn publish_candidate(
        handle: &Arc<RwLock<Self>>,
        candidate: Self,
        driver_tier: Option<&str>,
    ) -> std::result::Result<(), AdminError> {
        let update = Self::admin_update_lock(handle).await;
        Self::publish_candidate_owned(handle, candidate, driver_tier.map(str::to_string), update).await
    }

    fn begin_publish_transition_with_allowed_mutation_blocks(
        handle: &Arc<RwLock<Self>>,
        manager: &mut Self,
        candidate: &Self,
        mutation_block_allowance: Option<&MutationBlockAllowance>,
    ) -> std::result::Result<TierPublishTransition, AdminError> {
        let changed = changed_tier_names(manager, candidate);
        let replaced_destinations = replaced_tier_destinations(manager, candidate)?;
        Self::begin_tier_transition_with_destinations(handle, manager, changed, replaced_destinations, mutation_block_allowance)
    }

    fn begin_tier_transition_with_destinations(
        handle: &Arc<RwLock<Self>>,
        manager: &mut Self,
        changed: HashSet<String>,
        replaced_destinations: HashMap<String, TierConfig>,
        mutation_block_allowance: Option<&MutationBlockAllowance>,
    ) -> std::result::Result<TierPublishTransition, AdminError> {
        let runtime = tier_driver_runtime(handle, manager);
        for tier_name in replaced_destinations.keys() {
            if !lock_unpoisoned(&runtime).generations.contains_key(tier_name)
                && let Some(driver) = manager.driver_cache.remove(tier_name)
            {
                manager.replace_driver(tier_name, driver)?;
            }
        }
        Self::begin_tier_transition_in_runtime(runtime, changed, replaced_destinations, mutation_block_allowance)
    }

    fn begin_tier_transition_in_runtime(
        runtime: Arc<Mutex<TierDriverRuntime>>,
        changed: HashSet<String>,
        replaced_destinations: HashMap<String, TierConfig>,
        mutation_block_allowance: Option<&MutationBlockAllowance>,
    ) -> std::result::Result<TierPublishTransition, AdminError> {
        let count = u64::try_from(changed.len()).map_err(|_| {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier replacement count exceeds the supported limit".to_string();
            err
        })?;
        let mut runtime_guard = lock_unpoisoned(&runtime);
        if mutation_block_allowance.is_some_and(|allowance| runtime_guard.mutation_blocks_revision != allowance.revision) {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier mutation recovery changed before replacement".to_string();
            return Err(err);
        }
        if changed
            .iter()
            .any(|tier_name| runtime_blocks_tier_transition(&runtime_guard, tier_name, mutation_block_allowance))
        {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier configuration is already being replaced".to_string();
            return Err(err);
        }
        let final_epoch = runtime_guard.next_drain_epoch.checked_add(count).ok_or_else(|| {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier replacement epoch exhausted".to_string();
            err
        })?;
        let mut next_epoch = runtime_guard.next_drain_epoch;
        let mut tokens = Vec::with_capacity(changed.len());
        let mut revoked = HashMap::with_capacity(changed.len());
        for tier_name in changed {
            next_epoch += 1;
            runtime_guard.draining.insert(tier_name.clone(), next_epoch);
            tokens.push((tier_name.clone(), next_epoch));
            if let Some(generation) = runtime_guard.generations.remove(&tier_name) {
                generation.accepting.store(false, Ordering::Release);
                revoked.insert(tier_name, generation);
            }
        }
        runtime_guard.next_drain_epoch = final_epoch;
        drop(runtime_guard);
        Ok(TierPublishTransition {
            runtime,
            tokens,
            revoked,
            replaced_destinations,
            mutation_block_allowance: mutation_block_allowance.cloned(),
            published: false,
        })
    }

    async fn publish_candidate_inner_with_allowed_mutation_blocks(
        handle: &Arc<RwLock<Self>>,
        candidate: Self,
        driver_tier: Option<&str>,
        mutation_block_allowance: Option<&MutationBlockAllowance>,
    ) -> std::result::Result<(), AdminError> {
        let transition = {
            let mut manager = handle.write().await;
            Self::begin_publish_transition_with_allowed_mutation_blocks(
                handle,
                &mut manager,
                &candidate,
                mutation_block_allowance,
            )?
        };

        transition.wait_for_active_leases().await?;
        transition.ensure_replaced_destinations_are_empty().await?;
        Self::publish_candidate_after_drain(handle, candidate, driver_tier, transition).await
    }

    async fn publish_candidate_after_drain(
        handle: &Arc<RwLock<Self>>,
        mut candidate: Self,
        driver_tier: Option<&str>,
        mut transition: TierPublishTransition,
    ) -> std::result::Result<(), AdminError> {
        let prepared_driver =
            match driver_tier.and_then(|tier_name| candidate.driver_cache.remove(tier_name).map(|driver| (tier_name, driver))) {
                Some((tier_name, driver)) => {
                    let config = candidate.tiers.get(tier_name).ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?;
                    let config_fingerprint = tier_config_fingerprint(tier_name, config).map_err(|err| {
                        let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                        admin_err.message = err.to_string();
                        admin_err
                    })?;
                    let backend_identity = tier_backend_identity(config).map_err(|err| {
                        let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                        admin_err.message = err.to_string();
                        admin_err
                    })?;
                    let exact_get_delete = tier_exact_get_delete(config);
                    Some(PreparedTierDriver {
                        tier_name: tier_name.to_string(),
                        tier_config: config.clone_with_credentials(),
                        config_fingerprint,
                        backend_identity,
                        exact_get_delete,
                        driver: Arc::from(driver),
                    })
                }
                None => None,
            };
        // Lock order invariant: manager state before the per-manager driver runtime.
        let mut manager = handle.write().await;
        let mut runtime = lock_unpoisoned(&transition.runtime);
        if !transition
            .tokens
            .iter()
            .all(|(tier_name, epoch)| runtime.draining.get(tier_name) == Some(epoch))
        {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier replacement ownership was lost before publish".to_string();
            return Err(err);
        }
        let mutation_blocks_changed = transition
            .mutation_block_allowance
            .as_ref()
            .is_some_and(|allowance| runtime.mutation_blocks_revision != allowance.revision)
            || transition.tokens.iter().any(|(tier_name, _)| {
                runtime_mutation_blocks_tier_transition(&runtime, tier_name, transition.mutation_block_allowance.as_ref())
            });
        if mutation_blocks_changed {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier mutation recovery changed before publish".to_string();
            return Err(err);
        }
        let prepared_generation = if let Some(prepared) = prepared_driver {
            let generation = runtime.next_generation.checked_add(1).ok_or_else(|| {
                let mut err = ERR_TIER_INVALID_CONFIG.clone();
                err.message = "Remote tier driver generation exhausted".to_string();
                err
            })?;
            let entry = Arc::new(TierDriverGeneration {
                tier_name: Arc::from(prepared.tier_name.as_str()),
                tier_config: prepared.tier_config.clone_with_credentials(),
                generation,
                config_fingerprint: prepared.config_fingerprint,
                backend_identity: prepared.backend_identity,
                exact_get_delete: prepared.exact_get_delete,
                driver: prepared.driver.clone(),
                reconciler: tokio::sync::OnceCell::new(),
                accepting: AtomicBool::new(true),
                active_leases: AtomicUsize::new(0),
                drained: tokio::sync::Notify::new(),
            });
            Some((prepared, generation, entry))
        } else {
            None
        };
        for (tier_name, _) in &transition.tokens {
            manager.driver_cache.remove(tier_name);
        }
        manager.tiers = candidate.tiers;
        manager.last_refreshed_at = OffsetDateTime::now_utc();
        if let Some((prepared, generation, entry)) = prepared_generation {
            runtime.generations.insert(prepared.tier_name.clone(), entry);
            runtime.next_generation = generation;
            manager
                .driver_cache
                .insert(prepared.tier_name, Box::new(SharedWarmBackendProxy(prepared.driver)));
        }
        drop(runtime);
        transition.published = true;
        Ok(())
    }

    fn publish_task_error(err: tokio::task::JoinError) -> AdminError {
        error!(error = ?err, "remote tier configuration publish task failed");
        let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
        admin_err.message = format!("Remote tier configuration publish task failed: {err}");
        admin_err
    }

    #[allow(dead_code, reason = "reached only through #[cfg(test)] helpers in this file (backlog#1823)")]
    async fn publish_candidate_owned(
        handle: &Arc<RwLock<Self>>,
        candidate: Self,
        driver_tier: Option<String>,
        update: tokio::sync::OwnedMutexGuard<()>,
    ) -> std::result::Result<(), AdminError> {
        Self::publish_candidate_owned_with_allowed_mutation_blocks(handle, candidate, driver_tier, update, None).await
    }

    async fn publish_candidate_owned_with_allowed_mutation_blocks(
        handle: &Arc<RwLock<Self>>,
        candidate: Self,
        driver_tier: Option<String>,
        update: tokio::sync::OwnedMutexGuard<()>,
        mutation_block_allowance: Option<MutationBlockAllowance>,
    ) -> std::result::Result<(), AdminError> {
        let handle = handle.clone();
        tokio::spawn(async move {
            match AssertUnwindSafe(async move {
                let _update = update;
                Self::publish_candidate_inner_with_allowed_mutation_blocks(
                    &handle,
                    candidate,
                    driver_tier.as_deref(),
                    mutation_block_allowance.as_ref(),
                )
                .await
            })
            .catch_unwind()
            .await
            {
                Ok(Ok(())) => Ok(()),
                Ok(Err(err)) => {
                    error!(error = ?err, "remote tier configuration publish failed");
                    Err(err)
                }
                Err(_) => {
                    error!("remote tier configuration publish panicked");
                    let mut err = ERR_TIER_INVALID_CONFIG.clone();
                    err.message = "Remote tier configuration publish panicked".to_string();
                    Err(err)
                }
            }
        })
        .await
        .map_err(Self::publish_task_error)?
    }

    async fn update_candidate_owned<S>(
        handle: &Arc<RwLock<Self>>,
        api: Arc<S>,
        mut candidate: Self,
        version: Option<String>,
        mutation: TierCandidateMutation,
        update: tokio::sync::OwnedMutexGuard<()>,
        config_lock: Option<rustfs_lock::NamespaceLockGuard>,
    ) -> std::result::Result<(), TierConfigUpdateError>
    where
        S: TierReferenceProofStore + 'static,
    {
        let handle = handle.clone();
        #[cfg(test)]
        let test_peers = TIER_MUTATION_TEST_PEERS.try_with(|peers| peers.clone()).ok();
        #[cfg(test)]
        let test_driver_factory = TIER_DRIVER_TEST_FACTORY.try_with(|factory| factory.clone()).ok();
        #[cfg(test)]
        let test_reference_proof_barrier = TIER_REFERENCE_PROOF_TEST_BARRIER.try_with(Arc::clone).ok();
        tokio::spawn(async move {
            let update_task = async move {
                match AssertUnwindSafe(async move {
                    let mut config_lock = config_lock;
                    let coordinated_config_update = config_lock.is_some();
                    let mut update = Some(update);
                    let (mutation_kind, explicit_tier_name, mutation_force, current_for_targets, driver_tier, target_tiers) =
                        match mutation {
                            TierCandidateMutation::Prevalidated(prepared) => {
                                if version != prepared.version {
                                    return Err(TierConfigUpdateError::Load(io::Error::other(
                                        "tier configuration changed after remote backend validation",
                                    )));
                                }
                                if tier_config_candidate_digest(&prepared.candidate).map_err(TierConfigUpdateError::Save)?
                                    != prepared.candidate_digest
                                {
                                    return Err(TierConfigUpdateError::Save(io::Error::other(
                                        "prevalidated tier mutation candidate digest changed",
                                    )));
                                }
                                candidate = prepared.candidate;
                                let target_tiers = {
                                    let manager = handle.read().await;
                                    let mut target_tiers = changed_tier_names(&manager, &candidate);
                                    if let Some(tier_name) = prepared.explicit_tier_name.as_ref()
                                        && (manager.tiers.contains_key(tier_name)
                                            || prepared.current.tiers.contains_key(tier_name)
                                            || candidate.tiers.contains_key(tier_name))
                                    {
                                        target_tiers.insert(tier_name.clone());
                                    }
                                    target_tiers
                                };
                                (
                                    prepared.kind,
                                    prepared.explicit_tier_name,
                                    prepared.force,
                                    prepared.current,
                                    prepared.driver_tier,
                                    target_tiers,
                                )
                            }
                            mutation => {
                                let mutation_kind = mutation.intent_kind();
                                if version.is_none()
                                    && !candidate.tiers.is_empty()
                                    && mutation_kind != TierMutationIntentKind::Add
                                {
                                    return Err(TierConfigUpdateError::Load(io::Error::other(
                                        "tier configuration mutation requires an existing config ETag",
                                    )));
                                }
                                let explicit_tier_name = mutation.explicit_tier_name().map(str::to_string);
                                let mutation_force = mutation.force();
                                let current_for_targets = TierConfigMgr {
                                    driver_cache: HashMap::new(),
                                    tiers: candidate
                                        .tiers
                                        .iter()
                                        .map(|(tier_name, config)| (tier_name.clone(), config.clone_with_credentials()))
                                        .collect(),
                                    last_refreshed_at: candidate.last_refreshed_at,
                                };
                                let validation_deadline = Instant::now() + TIER_REMOTE_VALIDATION_TIMEOUT;
                                let target_tiers = {
                                    let manager = handle.read().await;
                                    mutation.target_tiers(&manager, &candidate)
                                };
                                let driver_tier = apply_tier_candidate_mutation(mutation, &mut candidate, validation_deadline)
                                    .await
                                    .map_err(TierConfigUpdateError::Mutation)?;
                                (
                                    mutation_kind,
                                    explicit_tier_name,
                                    mutation_force,
                                    current_for_targets,
                                    driver_tier,
                                    target_tiers,
                                )
                            }
                        };
                    let proof_targets = tier_mutation_proof_targets(
                        mutation_kind,
                        explicit_tier_name.as_deref(),
                        &current_for_targets,
                        &candidate,
                    );
                    let affected_targets =
                        build_tier_mutation_affected_targets(mutation_kind, proof_targets, &current_for_targets, &candidate)
                            .map_err(TierConfigUpdateError::Publish)?;
                    let coordinator_intent = build_coordinator_tier_mutation_intent(
                        mutation_kind,
                        version.clone(),
                        &candidate,
                        affected_targets.clone(),
                    )
                    .map_err(TierConfigUpdateError::Save)?;
                    save_coordinator_tier_mutation_intent(api.clone(), coordinator_intent.as_ref())
                        .await
                        .map_err(TierConfigUpdateError::Save)?;
                    let mut blocked_target_tiers = target_tiers.clone();
                    if let Some(intent) = coordinator_intent.as_ref() {
                        blocked_target_tiers.extend(intent.affected_targets.iter().map(|target| target.tier_name.clone()));
                    }
                    if let Some(intent) = coordinator_intent.as_ref() {
                        // `target_tiers` may include a stale local-only manager
                        // entry that is absent from the persisted proof
                        // snapshot. Fence that local transition under the same
                        // mutation ID as well; recovery may discard this
                        // process-local superset, which advances the revision
                        // and makes the deferred transition fail closed.
                        TierConfigMgr::apply_prepared_mutation_intent_block_for_tiers(&handle, intent, &blocked_target_tiers)
                            .await
                            .map_err(TierConfigUpdateError::Publish)?;
                    }
                    let prepared_mutation_block_allowance = match coordinator_intent.as_ref() {
                        Some(intent) => Some(
                            TierConfigMgr::mutation_block_allowance_for(&handle, intent.mutation_id)
                                .await
                                .map_err(TierConfigUpdateError::Publish)?,
                        ),
                        None => None,
                    };
                    // A durable coordinator intent supplies the admission
                    // fence that lets us defer generation revocation. Keep the
                    // original early transition for no-intent paths (for
                    // example, reconciling a stale local manager to an
                    // idempotently removed persisted tier), where there is no
                    // Prepared record capable of blocking a new lease.
                    let (mut transition, deferred_target_tiers) = if coordinator_intent.is_some() {
                        (None, Some(target_tiers))
                    } else {
                        let transition = {
                            let mut manager = handle.write().await;
                            Self::begin_tier_transition_with_destinations(
                                &handle,
                                &mut manager,
                                target_tiers,
                                HashMap::new(),
                                None,
                            )
                            .map_err(TierConfigUpdateError::Publish)?
                        };
                        (Some(transition), None)
                    };
                    if coordinated_config_update {
                        drop(update.take());
                        drop(config_lock.take());
                    }
                    // The durable Prepared block closes admission before the
                    // zero-reference proof, but deliberately leaves already
                    // issued generations current. In particular, an exact
                    // free-version cleanup that has completed remote DELETE
                    // must still be able to remove its local ownership marker;
                    // revoking its generation here would strand that marker
                    // and make this mutation reject its own interrupted work.
                    if let Some(intent) = coordinator_intent.as_ref()
                        && let Err(drain_error) =
                            TierConfigMgr::wait_for_blocked_tier_operation_leases_for_tiers(&handle, &blocked_target_tiers).await
                    {
                        if !abort_prepared_tier_mutation(&handle, api.clone(), Some(intent), Vec::new()).await {
                            warn!(
                                event = "tier_mutation_abort",
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_TIER,
                                result = "prepared_intent_retained",
                                mutation_id = %intent.mutation_id,
                                "tier mutation lease drain failed and abort was incomplete"
                            );
                        }
                        return Err(TierConfigUpdateError::Publish(drain_error));
                    } else if let Some(transition) = transition.as_ref() {
                        let drain_deadline = Instant::now() + TIER_REMOTE_VALIDATION_TIMEOUT;
                        transition
                            .wait_for_active_leases_until(drain_deadline)
                            .await
                            .map_err(TierConfigUpdateError::Publish)?;
                    }
                    let prepared_peers = if let Some(intent) = coordinator_intent.as_ref() {
                        let peers = match remote_tier_mutation_peers().await {
                            Ok(peers) => peers,
                            Err(err) => {
                                if !abort_prepared_tier_mutation(&handle, api.clone(), coordinator_intent.as_ref(), Vec::new())
                                    .await
                                {
                                    warn!(
                                        event = "tier_mutation_abort",
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_TIER,
                                        result = "prepared_intent_retained",
                                        mutation_id = %intent.mutation_id,
                                        "tier mutation peer discovery failed and local abort was incomplete"
                                    );
                                }
                                return Err(TierConfigUpdateError::Publish(tier_mutation_fanout_admin_error("prepare", err)));
                            }
                        };
                        if peers.is_empty() {
                            Vec::new()
                        } else {
                            match prepare_tier_mutation_peers(intent.mutation_id, peers, intent).await {
                                Ok(prepared) => prepared,
                                Err(failure) => {
                                    if !abort_prepared_tier_mutation(
                                        &handle,
                                        api.clone(),
                                        coordinator_intent.as_ref(),
                                        failure.prepared_peers,
                                    )
                                    .await
                                    {
                                        warn!(
                                            event = "tier_mutation_abort",
                                            component = LOG_COMPONENT_ECSTORE,
                                            subsystem = LOG_SUBSYSTEM_TIER,
                                            result = "prepared_intent_retained",
                                            mutation_id = %intent.mutation_id,
                                            "tier mutation abort incomplete"
                                        );
                                    }
                                    return Err(TierConfigUpdateError::Publish(failure.error));
                                }
                            }
                        }
                    } else {
                        Vec::new()
                    };
                    if let Err(proof_error) =
                        ensure_no_authoritative_tier_object_references(api.clone(), &affected_targets, mutation_force).await
                    {
                        if !abort_prepared_tier_mutation(&handle, api.clone(), coordinator_intent.as_ref(), prepared_peers).await
                        {
                            warn!(
                                event = "tier_mutation_abort",
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_TIER,
                                result = "prepared_intent_retained",
                                coordinator_intent = coordinator_intent.is_some(),
                                "tier mutation reference proof failed and abort was incomplete"
                            );
                        }
                        return Err(TierConfigUpdateError::Publish(proof_error));
                    }
                    // No affected-tier lease can start after Prepared, and the
                    // existing set was drained above. It is now safe to revoke
                    // the generation for publication without invalidating a
                    // cleanup between its remote and local commit boundaries.
                    if transition.is_none() {
                        let target_tiers = deferred_target_tiers.ok_or_else(|| {
                            let mut err = ERR_TIER_INVALID_CONFIG.clone();
                            err.message = "Remote tier mutation lost its deferred transition targets".to_string();
                            TierConfigUpdateError::Publish(err)
                        })?;
                        let mut manager = handle.write().await;
                        transition = Some(
                            match Self::begin_tier_transition_with_destinations(
                                &handle,
                                &mut manager,
                                target_tiers,
                                HashMap::new(),
                                prepared_mutation_block_allowance.as_ref(),
                            ) {
                                Ok(transition) => transition,
                                Err(transition_error) => {
                                    drop(manager);
                                    if !abort_prepared_tier_mutation(
                                        &handle,
                                        api.clone(),
                                        coordinator_intent.as_ref(),
                                        prepared_peers,
                                    )
                                    .await
                                    {
                                        warn!(
                                            event = "tier_mutation_abort",
                                            component = LOG_COMPONENT_ECSTORE,
                                            subsystem = LOG_SUBSYSTEM_TIER,
                                            result = "prepared_intent_retained",
                                            coordinator_intent = coordinator_intent.is_some(),
                                            "tier mutation publish transition failed and abort was incomplete"
                                        );
                                    }
                                    return Err(TierConfigUpdateError::Publish(transition_error));
                                }
                            },
                        );
                    }
                    let mut transition = transition.ok_or_else(|| {
                        let mut err = ERR_TIER_INVALID_CONFIG.clone();
                        err.message = "Remote tier mutation lost its publish transition".to_string();
                        TierConfigUpdateError::Publish(err)
                    })?;
                    let drain_deadline = Instant::now() + TIER_REMOTE_VALIDATION_TIMEOUT;
                    if let Err(drain_error) = transition.wait_for_active_leases_until(drain_deadline).await {
                        drop(transition);
                        if !abort_prepared_tier_mutation(&handle, api.clone(), coordinator_intent.as_ref(), prepared_peers).await
                        {
                            warn!(
                                event = "tier_mutation_abort",
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_TIER,
                                result = "prepared_intent_retained",
                                coordinator_intent = coordinator_intent.is_some(),
                                "tier mutation publish drain failed and abort was incomplete"
                            );
                        }
                        return Err(TierConfigUpdateError::Publish(drain_error));
                    }
                    let candidate_digest = tier_config_candidate_digest(&candidate).map_err(TierConfigUpdateError::Save)?;
                    if coordinated_config_update {
                        config_lock = match Self::acquire_tier_config_write_lock(api.clone()).await {
                            Ok(config_lock) => Some(config_lock),
                            Err(lock_error) => {
                                if !abort_prepared_tier_mutation(
                                    &handle,
                                    api.clone(),
                                    coordinator_intent.as_ref(),
                                    prepared_peers,
                                )
                                .await
                                {
                                    warn!(
                                        event = "tier_mutation_abort",
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_TIER,
                                        result = "prepared_intent_retained",
                                        coordinator_intent = coordinator_intent.is_some(),
                                        "tier mutation config-lock reacquisition failed and abort was incomplete"
                                    );
                                }
                                return Err(lock_error);
                            }
                        };
                        update = Some(Self::admin_update_lock(&handle).await);
                        if let Err(revalidation_error) = Self::revalidate_prepared_candidate_before_commit(
                            api.clone(),
                            version.as_deref(),
                            candidate_digest,
                            coordinator_intent.as_ref(),
                        )
                        .await
                        {
                            drop(update.take());
                            drop(config_lock.take());
                            if !abort_prepared_tier_mutation(&handle, api.clone(), coordinator_intent.as_ref(), prepared_peers)
                                .await
                            {
                                warn!(
                                    event = "tier_mutation_abort",
                                    component = LOG_COMPONENT_ECSTORE,
                                    subsystem = LOG_SUBSYSTEM_TIER,
                                    result = "prepared_intent_retained",
                                    coordinator_intent = coordinator_intent.is_some(),
                                    "tier mutation revalidation failed and abort was incomplete"
                                );
                            }
                            return Err(TierConfigUpdateError::Save(revalidation_error));
                        }
                    }
                    if update.is_none() {
                        return Err(TierConfigUpdateError::Save(io::Error::other(
                            "tier mutation lost local serialization before config commit",
                        )));
                    }
                    if coordinated_config_update && config_lock.is_none() {
                        return Err(TierConfigUpdateError::Save(io::Error::other(
                            "tier mutation lost namespace serialization before config commit",
                        )));
                    }
                    let saved = match candidate
                        .save_tiering_config_if_current_with_info(api.clone(), version.as_deref())
                        .await
                    {
                        Ok(saved) => saved,
                        Err(save_err) => match load_tier_config_for_recovery(api.clone()).await {
                            Ok(loaded) => match loaded_tier_config_matches_candidate_digest(&loaded, candidate_digest) {
                                Ok(true) => ObjectInfo {
                                    etag: loaded.etag,
                                    ..Default::default()
                                },
                                Ok(false) => {
                                    drop(update.take());
                                    drop(config_lock.take());
                                    let aborted = abort_prepared_tier_mutation(
                                        &handle,
                                        api.clone(),
                                        coordinator_intent.as_ref(),
                                        prepared_peers,
                                    )
                                    .await;
                                    if !aborted {
                                        warn!(
                                            event = "tier_mutation_abort",
                                            component = LOG_COMPONENT_ECSTORE,
                                            subsystem = LOG_SUBSYSTEM_TIER,
                                            result = "prepared_intent_retained",
                                            coordinator_intent = coordinator_intent.is_some(),
                                            "tier mutation abort incomplete"
                                        );
                                    }
                                    return Err(TierConfigUpdateError::Save(save_err));
                                }
                                Err(read_err) => {
                                    TierConfigMgr::request_committed_mutation_refresh(&handle).await;
                                    let err = io::Error::other(format!(
                                        "tier configuration save outcome is unknown; read-back validation failed: {read_err}"
                                    ));
                                    return Err(TierConfigUpdateError::Save(err));
                                }
                            },
                            Err(read_err) => {
                                TierConfigMgr::request_committed_mutation_refresh(&handle).await;
                                let err = io::Error::other(format!(
                                    "tier configuration save outcome is unknown; read-back failed: {read_err}"
                                ));
                                return Err(TierConfigUpdateError::Save(err));
                            }
                        },
                    };
                    let committed_config_etag = match saved.etag.filter(|etag| !etag.trim().is_empty()) {
                        Some(etag) => etag,
                        None => {
                            TierConfigMgr::request_committed_mutation_refresh(&handle).await;
                            return Err(TierConfigUpdateError::Save(io::Error::other(
                                "tier configuration object is missing an ETag after save",
                            )));
                        }
                    };
                    let committed_coordinator_intent =
                        committed_tier_mutation_intent(coordinator_intent.as_ref(), &committed_config_etag)
                            .map_err(TierConfigUpdateError::Save)?;
                    // Persist Committed before notifying refresh; a Prepared disk record
                    // would restore the prepared block and invalidate our publish allowance.
                    let coordinator_commit =
                        commit_coordinator_tier_mutation_intent(api.clone(), coordinator_intent.as_ref(), &committed_config_etag)
                            .await;
                    if let Some(intent) = committed_coordinator_intent.as_ref() {
                        TierConfigMgr::apply_committed_mutation_intent_block(&handle, intent)
                            .await
                            .map_err(TierConfigUpdateError::Publish)?;
                        transition.mutation_block_allowance = Some(
                            TierConfigMgr::mutation_block_allowance_for(&handle, intent.mutation_id)
                                .await
                                .map_err(TierConfigUpdateError::Publish)?,
                        );
                    }
                    // Config is already saved: retain the committed fence and wake recovery
                    // even when the coordinator commit failed or its outcome is unknown.
                    coordinator_commit.map_err(TierConfigUpdateError::Save)?;
                    if coordinated_config_update {
                        drop(update.take());
                        drop(config_lock.take());
                    }
                    if let Some(intent) = committed_coordinator_intent.as_ref()
                        && !prepared_peers.is_empty()
                    {
                        commit_tier_mutation_peers(intent.mutation_id, prepared_peers, &committed_config_etag)
                            .await
                            .map_err(|err| TierConfigUpdateError::Publish(tier_mutation_fanout_admin_error("commit", err)))?;
                    }
                    Self::publish_candidate_after_drain(&handle, candidate, driver_tier.as_deref(), transition)
                        .await
                        .map_err(TierConfigUpdateError::Publish)?;
                    if let Some(intent) = committed_coordinator_intent.as_ref() {
                        Self::clear_committed_mutation_intent_block(&handle, intent.mutation_id)
                            .await
                            .map_err(TierConfigUpdateError::Publish)?;
                    }
                    Ok(())
                })
                .catch_unwind()
                .await
                {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(err)) => {
                        error!(error = ?err, "remote tier configuration update failed");
                        Err(err)
                    }
                    Err(_) => {
                        error!("remote tier configuration update panicked");
                        let mut err = ERR_TIER_INVALID_CONFIG.clone();
                        err.message = "Remote tier configuration update panicked".to_string();
                        Err(TierConfigUpdateError::Publish(err))
                    }
                }
            };
            #[cfg(test)]
            {
                let mut update_task = update_task.boxed();
                if let Some(barrier) = test_reference_proof_barrier {
                    update_task = TIER_REFERENCE_PROOF_TEST_BARRIER.scope(barrier, update_task).boxed();
                }
                if let Some(peers) = test_peers {
                    update_task = TIER_MUTATION_TEST_PEERS.scope(peers, update_task).boxed();
                }
                if let Some(factory) = test_driver_factory {
                    update_task = TIER_DRIVER_TEST_FACTORY.scope(factory, update_task).boxed();
                }
                update_task.await
            }
            #[cfg(not(test))]
            {
                update_task.await
            }
        })
        .await
        .map_err(|err| TierConfigUpdateError::Publish(Self::publish_task_error(err)))?
    }

    pub async fn reload_handle(handle: &Arc<RwLock<Self>>, api: Arc<ECStore>) -> io::Result<()> {
        Self::reload_handle_with(handle, api).await
    }

    async fn reload_handle_with<S>(handle: &Arc<RwLock<Self>>, api: Arc<S>) -> io::Result<()>
    where
        S: EcstoreObjectIO
            + EcstoreObjectOperations
            + NamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>
            + ListOperations<Error = Error, ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>>,
    {
        // Lock order matches update_candidate_with_config_lock: namespace tier-config lock before admin_updates.
        let mut config_lock: Option<rustfs_lock::NamespaceLockGuard> = None;
        let (snapshot, candidate, recovery_plan, update) = loop {
            let mut snapshot = Self::load_and_reconcile_mutation_recovery_snapshot(handle, api.clone(), true).await?;
            if snapshot.requires_config_lock() && config_lock.is_none() {
                config_lock = Some(
                    Self::acquire_tier_config_write_lock(api.clone())
                        .await
                        .map_err(|err| match err {
                            TierConfigUpdateError::Load(err) => err,
                            other => io::Error::other(format!("{other:?}")),
                        })?,
                );
                continue;
            }

            Self::reconcile_terminal_dual_prefix_mutation_intents(api.clone(), &mut snapshot).await?;
            Self::recover_prepared_coordinator_mutation_intents(api.clone(), &mut snapshot.coordinator_intents).await?;

            snapshot = Self::load_and_reconcile_mutation_recovery_snapshot(handle, api.clone(), true).await?;
            if snapshot.requires_config_lock() && config_lock.is_none() {
                config_lock = Some(
                    Self::acquire_tier_config_write_lock(api.clone())
                        .await
                        .map_err(|err| match err {
                            TierConfigUpdateError::Load(err) => err,
                            other => io::Error::other(format!("{other:?}")),
                        })?,
                );
                continue;
            }

            let preloaded_recovery = if config_lock.is_some() {
                let (candidate, current_config_etag) = load_tier_config_for_update(api.clone()).await?;
                let recovery_plan = Self::plan_committed_mutation_recovery(&snapshot, current_config_etag.as_deref())?;
                Self::replay_recovered_committed_mutation_intents(&snapshot, &recovery_plan).await?;
                Some((candidate, recovery_plan))
            } else {
                None
            };

            let update = Self::admin_update_lock(handle).await;
            if Self::mutation_blocks_revision(handle).await != snapshot.allowance.revision {
                drop(update);
                continue;
            }
            let (candidate, recovery_plan) = match preloaded_recovery {
                Some(recovery) => recovery,
                None => {
                    let (candidate, current_config_etag) = load_tier_config_for_update(api.clone()).await?;
                    let recovery_plan = Self::plan_committed_mutation_recovery(&snapshot, current_config_etag.as_deref())?;
                    Self::replay_recovered_committed_mutation_intents(&snapshot, &recovery_plan).await?;
                    (candidate, recovery_plan)
                }
            };
            break (snapshot, candidate, recovery_plan, update);
        };
        let allowance = snapshot.allowance();
        Self::publish_candidate_owned_with_allowed_mutation_blocks(handle, candidate, None, update, Some(allowance))
            .await
            .map_err(io::Error::other)?;
        Self::delete_finished_coordinator_mutation_intents(api.clone(), &snapshot, &recovery_plan.coordinator_cleanup_order)
            .await?;
        Self::delete_finished_peer_mutation_intents(api.clone(), &snapshot, &recovery_plan.peer_cleanup_order).await?;
        if !snapshot.is_quiescent() {
            let settled_snapshot = Self::load_and_reconcile_mutation_recovery_snapshot(handle, api.clone(), false).await?;
            Self::delete_finished_peer_mutation_intents(api, &settled_snapshot, &[]).await?;
        }
        drop(config_lock);
        Ok(())
    }

    async fn load_and_reconcile_mutation_recovery_snapshot<S>(
        handle: &Arc<RwLock<Self>>,
        api: Arc<S>,
        retain_missing_mutation_blocks: bool,
    ) -> io::Result<TierMutationRecoverySnapshot>
    where
        S: EcstoreObjectIO + ListOperations<Error = Error, ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>>,
    {
        for _ in 0..3 {
            let base_revision = Self::mutation_blocks_revision(handle).await;
            let peer_intents = Self::load_tier_mutation_intents(api.clone()).await?;
            let coordinator_intents = Self::load_coordinator_mutation_intents(api.clone()).await?;
            let intents = Self::merge_mutation_recovery_intents(&peer_intents, &coordinator_intents)?;
            if let Some((allowance, has_committed_mutation_blocks)) = Self::reconcile_mutation_intent_blocks_with_revision(
                handle,
                &intents,
                base_revision,
                retain_missing_mutation_blocks,
            )
            .await
            .map_err(io::Error::other)?
            {
                return Ok(TierMutationRecoverySnapshot {
                    coordinator_intents,
                    intents,
                    allowance,
                    has_committed_mutation_blocks,
                });
            }
        }
        Err(io::Error::other("tier mutation block recovery changed repeatedly during reload"))
    }

    fn merge_mutation_recovery_intents(
        peer_intents: &[TierMutationIntent],
        coordinator_intents: &[TierMutationIntent],
    ) -> io::Result<Vec<RecoveredTierMutationIntent>> {
        let peers = Self::mutation_intents_by_id(peer_intents, "peer")?;
        let coordinators = Self::mutation_intents_by_id(coordinator_intents, "coordinator")?;
        let mut mutation_ids = peers.keys().chain(coordinators.keys()).copied().collect::<Vec<_>>();
        mutation_ids.sort_unstable();
        mutation_ids.dedup();

        let mut recovered = Vec::with_capacity(mutation_ids.len());
        for mutation_id in mutation_ids {
            let peer = peers.get(&mutation_id);
            let coordinator = coordinators.get(&mutation_id);
            let intent = match (peer, coordinator) {
                (Some(peer), Some(coordinator)) => Self::merge_dual_prefix_mutation_intent(peer, coordinator)?,
                (Some(peer), None) => (*peer).clone(),
                (None, Some(coordinator)) => (*coordinator).clone(),
                (None, None) => continue,
            };
            recovered.push(RecoveredTierMutationIntent {
                intent,
                has_peer_record: peer.is_some(),
                has_coordinator_record: coordinator.is_some(),
                peer_state: peer.map(|intent| intent.state),
                coordinator_state: coordinator.map(|intent| intent.state),
            });
        }
        Ok(recovered)
    }

    fn mutation_intents_by_id<'a>(
        intents: &'a [TierMutationIntent],
        source: &str,
    ) -> io::Result<HashMap<uuid::Uuid, &'a TierMutationIntent>> {
        let mut by_id = HashMap::with_capacity(intents.len());
        for intent in intents {
            if let Some(existing) = by_id.insert(intent.mutation_id, intent)
                && existing != intent
            {
                return Err(io::Error::other(format!(
                    "conflicting {source} tier mutation intent records share mutation_id {}",
                    intent.mutation_id
                )));
            }
        }
        Ok(by_id)
    }

    fn merge_dual_prefix_mutation_intent(
        peer: &TierMutationIntent,
        coordinator: &TierMutationIntent,
    ) -> io::Result<TierMutationIntent> {
        if !peer.same_identity_as(coordinator) {
            return Err(io::Error::other(format!(
                "peer and coordinator tier mutation intent {} have conflicting identities",
                peer.mutation_id
            )));
        }
        if peer == coordinator {
            return Ok(peer.clone());
        }
        let (prepared, terminal) = match (peer.state, coordinator.state) {
            (TierMutationIntentState::Prepared, TierMutationIntentState::Committed | TierMutationIntentState::Aborted) => {
                (peer, coordinator)
            }
            (TierMutationIntentState::Aborted, TierMutationIntentState::Prepared) => (coordinator, peer),
            (TierMutationIntentState::Aborted, TierMutationIntentState::Committed) => return Ok(coordinator.clone()),
            _ => {
                return Err(io::Error::other(format!(
                    "peer and coordinator tier mutation intent {} have conflicting states",
                    peer.mutation_id
                )));
            }
        };
        let mut advanced = prepared.clone();
        advanced
            .advance(terminal.state, terminal.committed_config_etag.clone())
            .map_err(io::Error::other)?;
        if advanced == *terminal {
            return Ok(terminal.clone());
        }
        Err(io::Error::other(format!(
            "peer and coordinator tier mutation intent {} have conflicting states",
            peer.mutation_id
        )))
    }

    fn tier_mutation_tombstone_retention_elapsed(intent: &TierMutationIntent, now_unix_nanos: i64) -> bool {
        let clock_skew_nanos = i64::try_from(TIER_MUTATION_TOMBSTONE_CLOCK_SKEW.as_nanos()).unwrap_or(i64::MAX);
        intent.expires_at_unix_nanos.saturating_add(clock_skew_nanos) <= now_unix_nanos
    }

    async fn delete_exact_peer_terminal_mutation_intent<S>(api: Arc<S>, expected: &TierMutationIntent) -> io::Result<bool>
    where
        S: EcstoreObjectIO + EcstoreObjectOperations,
    {
        if expected.state == TierMutationIntentState::Prepared {
            return Err(io::Error::other("refusing to delete a non-terminal peer tier mutation intent"));
        }
        let (current, etag) = match load_tier_mutation_intent_record_with_etag(api.clone(), expected.mutation_id).await {
            Ok(current) => current,
            Err(Error::ConfigNotFound) => return Ok(true),
            Err(err) => return Err(tier_mutation_replay_error(err)),
        };
        if current != *expected {
            return Err(stable_io_error(
                "peer tier mutation intent changed before terminal cleanup",
                expected.mutation_id.to_string(),
            ));
        }
        match delete_tier_mutation_intent_record_if_current(api, expected.mutation_id, &etag).await {
            Ok(()) | Err(Error::ConfigNotFound) => Ok(true),
            Err(Error::PreconditionFailed) => Ok(false),
            Err(err) => Err(tier_mutation_replay_error(err)),
        }
    }

    async fn delete_exact_coordinator_terminal_mutation_intent<S>(api: Arc<S>, expected: &TierMutationIntent) -> io::Result<bool>
    where
        S: EcstoreObjectIO + EcstoreObjectOperations,
    {
        if expected.state == TierMutationIntentState::Prepared {
            return Err(io::Error::other("refusing to delete a non-terminal coordinator tier mutation intent"));
        }
        let (current, etag) =
            match load_tier_coordinator_mutation_intent_record_with_etag(api.clone(), expected.mutation_id).await {
                Ok(current) => current,
                Err(Error::ConfigNotFound) => return Ok(true),
                Err(err) => return Err(tier_mutation_replay_error(err)),
            };
        if current != *expected {
            return Err(stable_io_error(
                "coordinator tier mutation intent changed before terminal cleanup",
                expected.mutation_id.to_string(),
            ));
        }
        match delete_tier_coordinator_mutation_intent_record_if_current(api, expected.mutation_id, &etag).await {
            Ok(()) | Err(Error::ConfigNotFound) => Ok(true),
            Err(Error::PreconditionFailed) => Ok(false),
            Err(err) => Err(tier_mutation_replay_error(err)),
        }
    }

    async fn gc_peer_terminal_mutation_intent<S>(api: Arc<S>, mutation_id: uuid::Uuid, now_unix_nanos: i64) -> io::Result<()>
    where
        S: EcstoreObjectIO + EcstoreObjectOperations,
    {
        let (current, etag) = match load_tier_mutation_intent_record_with_etag(api.clone(), mutation_id).await {
            Ok(current) => current,
            Err(Error::ConfigNotFound) => return Ok(()),
            Err(err) => return Err(tier_mutation_replay_error(err)),
        };
        if current.state == TierMutationIntentState::Prepared
            || !Self::tier_mutation_tombstone_retention_elapsed(&current, now_unix_nanos)
        {
            return Ok(());
        }
        match load_tier_coordinator_mutation_intent_record_with_etag(api.clone(), mutation_id).await {
            Ok(_) => return Ok(()),
            Err(Error::ConfigNotFound) => {}
            Err(err) => return Err(tier_mutation_replay_error(err)),
        }
        match delete_tier_mutation_intent_record_if_current(api, mutation_id, &etag).await {
            Ok(()) | Err(Error::ConfigNotFound | Error::PreconditionFailed) => Ok(()),
            Err(err) => Err(tier_mutation_replay_error(err)),
        }
    }

    async fn reconcile_terminal_dual_prefix_mutation_intents<S>(
        api: Arc<S>,
        snapshot: &mut TierMutationRecoverySnapshot,
    ) -> io::Result<()>
    where
        S: EcstoreObjectIO + EcstoreObjectOperations,
    {
        let terminal = snapshot
            .intents
            .iter()
            .filter(|recovered| {
                recovered.has_peer_record
                    && recovered.has_coordinator_record
                    && recovered.intent.state != TierMutationIntentState::Prepared
            })
            .map(|recovered| recovered.intent.clone())
            .collect::<Vec<_>>();
        let mut peers: Option<Vec<Arc<dyn TierMutationPeer>>> = None;
        for intent in terminal {
            let recovered = snapshot
                .intents
                .iter()
                .find(|recovered| recovered.intent.mutation_id == intent.mutation_id)
                .ok_or_else(|| io::Error::other("terminal tier mutation recovery lost its source record"))?;
            let stale_aborted_peer = recovered.peer_state == Some(TierMutationIntentState::Aborted)
                && recovered.coordinator_state == Some(TierMutationIntentState::Committed);
            {
                let _mutation_guard = acquire_tier_mutation_mutex(intent.mutation_id).await;
                if stale_aborted_peer {
                    let mut peer = intent.original_prepared().map_err(io::Error::other)?;
                    peer.advance(TierMutationIntentState::Aborted, None)
                        .map_err(io::Error::other)?;
                    if !Self::delete_exact_peer_terminal_mutation_intent(api.clone(), &peer).await? {
                        return Err(io::Error::other("stale aborted peer intent changed during conditional cleanup"));
                    }
                    continue;
                }
                advance_tier_mutation_intent_record_idempotent(
                    api.clone(),
                    intent.mutation_id,
                    intent.state,
                    intent.committed_config_etag.clone(),
                )
                .await
                .map_err(io::Error::other)?;
            }

            if intent.state == TierMutationIntentState::Aborted {
                let resolved = match &peers {
                    Some(peers) => peers.clone(),
                    None => {
                        let resolved = remote_tier_mutation_peers().await?;
                        peers = Some(resolved.clone());
                        resolved
                    }
                };
                abort_tier_mutation_peers(&intent, resolved).await?;
                if let Some(coordinator) = snapshot
                    .coordinator_intents
                    .iter_mut()
                    .find(|coordinator| coordinator.mutation_id == intent.mutation_id)
                    && coordinator.state == TierMutationIntentState::Prepared
                {
                    let _mutation_guard = acquire_tier_mutation_mutex(intent.mutation_id).await;
                    let (advanced, _) = advance_tier_coordinator_mutation_intent_record_idempotent(
                        api.clone(),
                        intent.mutation_id,
                        TierMutationIntentState::Aborted,
                        None,
                    )
                    .await
                    .map_err(io::Error::other)?;
                    *coordinator = advanced;
                }
            }
        }
        Ok(())
    }

    async fn load_tier_mutation_intents<S>(api: Arc<S>) -> io::Result<Vec<TierMutationIntent>>
    where
        S: EcstoreObjectIO + ListOperations<Error = Error, ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>>,
    {
        let mut marker = None;
        let mut intents = Vec::new();
        loop {
            let scan = list_tier_mutation_intent_records(api.clone(), TIER_MUTATION_INTENT_RECOVERY_SCAN_LIMIT, marker)
                .await
                .map_err(io::Error::other)?;
            if scan.failed != 0 {
                return Err(io::Error::other(format!(
                    "failed to scan {} tier mutation intent record(s) during recovery",
                    scan.failed
                )));
            }
            intents.extend(scan.intents);
            if !scan.truncated {
                return Ok(intents);
            }
            let next_marker = scan.next_marker.ok_or_else(|| {
                io::Error::other("tier mutation intent recovery scan was truncated without a continuation marker")
            })?;
            marker = Some(next_marker);
        }
    }

    #[cfg(test)]
    async fn load_prepared_coordinator_mutation_intents<S>(api: Arc<S>) -> io::Result<Vec<TierMutationIntent>>
    where
        S: EcstoreObjectIO + ListOperations<Error = Error, ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>>,
    {
        let mut intents = Self::load_coordinator_mutation_intents(api).await?;
        intents.retain(|intent| intent.state == TierMutationIntentState::Prepared);
        Ok(intents)
    }

    async fn load_coordinator_mutation_intents<S>(api: Arc<S>) -> io::Result<Vec<TierMutationIntent>>
    where
        S: EcstoreObjectIO + ListOperations<Error = Error, ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>>,
    {
        let mut marker = None;
        let mut intents = Vec::new();
        loop {
            let scan =
                list_tier_coordinator_mutation_intent_records(api.clone(), TIER_MUTATION_INTENT_RECOVERY_SCAN_LIMIT, marker)
                    .await
                    .map_err(io::Error::other)?;
            if scan.failed != 0 {
                return Err(io::Error::other(format!(
                    "failed to scan {} coordinator tier mutation intent record(s) during recovery",
                    scan.failed
                )));
            }
            intents.extend(scan.intents);
            if !scan.truncated {
                return Ok(intents);
            }
            let next_marker = scan.next_marker.ok_or_else(|| {
                io::Error::other("coordinator tier mutation intent recovery scan was truncated without a continuation marker")
            })?;
            marker = Some(next_marker);
        }
    }

    async fn recover_prepared_coordinator_mutation_intents<S>(api: Arc<S>, intents: &mut [TierMutationIntent]) -> io::Result<()>
    where
        S: EcstoreObjectIO,
    {
        if !intents.iter().any(|intent| intent.state == TierMutationIntentState::Prepared) {
            return Ok(());
        }
        let loaded = load_tier_config_for_recovery(api.clone()).await?;
        let persisted_digest = loaded.persisted_digest;
        let current_digest = if intents
            .iter()
            .any(|intent| intent.state == TierMutationIntentState::Prepared && persisted_digest != Some(intent.candidate_digest))
        {
            Some(tier_config_candidate_digest(&loaded.config)?)
        } else {
            None
        };
        let now = i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap_or(i64::MAX);
        let mut peers: Option<Vec<Arc<dyn TierMutationPeer>>> = None;
        for intent in intents
            .iter_mut()
            .filter(|intent| intent.state == TierMutationIntentState::Prepared)
        {
            if persisted_digest == Some(intent.candidate_digest) || current_digest == Some(intent.candidate_digest) {
                let config_etag = loaded
                    .etag
                    .as_ref()
                    .ok_or_else(|| io::Error::other("matching coordinator intent has no tier config ETag"))?;
                let _mutation_guard = acquire_tier_mutation_mutex(intent.mutation_id).await;
                let (advanced, _) = advance_tier_coordinator_mutation_intent_record_idempotent(
                    api.clone(),
                    intent.mutation_id,
                    TierMutationIntentState::Committed,
                    Some(config_etag.clone()),
                )
                .await
                .map_err(io::Error::other)?;
                *intent = advanced;
                continue;
            }

            let abort_proof_matches = match intent.old_config_etag.as_deref() {
                Some(old_config_etag) => loaded.etag.as_deref() == Some(old_config_etag),
                None => intent.kind == TierMutationIntentKind::Add,
            };
            if !abort_proof_matches {
                // A third configuration is neither the candidate nor the old
                // configuration. Its history is uncertain, so retain the
                // Prepared record and runtime block for a later proof.
                continue;
            }

            // The old config remaining current does not prove that a
            // Prepared mutation was abandoned: its live coordinator may be
            // performing peer fanout or the reference proof without holding
            // the namespace lock. Preserve every unexpired durable fence.
            if intent.expires_at_unix_nanos > now {
                continue;
            }

            let peers = match &peers {
                Some(peers) => peers.clone(),
                None => {
                    let resolved = remote_tier_mutation_peers().await?;
                    peers = Some(resolved.clone());
                    resolved
                }
            };
            abort_tier_mutation_peers(intent, peers)
                .await
                .map_err(|err| io::Error::other(format!("expired coordinator tier mutation recovery abort failed: {err}")))?;
            let _mutation_guard = acquire_tier_mutation_mutex(intent.mutation_id).await;
            let (advanced, _) = advance_tier_coordinator_mutation_intent_record_idempotent(
                api.clone(),
                intent.mutation_id,
                TierMutationIntentState::Aborted,
                None,
            )
            .await
            .map_err(io::Error::other)?;
            *intent = advanced;
        }
        Ok(())
    }

    fn plan_committed_mutation_recovery(
        snapshot: &TierMutationRecoverySnapshot,
        current_config_etag: Option<&str>,
    ) -> io::Result<CommittedMutationRecoveryPlan> {
        let current_config_etag = current_config_etag.filter(|etag| !etag.is_empty());
        if snapshot.has_committed_mutation_blocks && current_config_etag.is_none() {
            return Err(io::Error::other("committed tier mutation recovery requires the current config ETag"));
        }
        let mut current_coordinator_only = Vec::new();
        let mut peer_backed = Vec::new();
        let mut stale_coordinator_only = Vec::new();
        let mut current_or_dual_coordinator = Vec::new();
        let mut peer_cleanup_order = Vec::new();
        for recovered in snapshot.committed_intents() {
            let intent = &recovered.intent;
            let destination = recovered
                .intent
                .committed_config_etag
                .as_ref()
                .ok_or_else(|| io::Error::other("committed tier mutation intent is missing committed config etag"))?;
            if recovered.has_peer_record {
                peer_cleanup_order.push(intent.mutation_id);
                peer_backed.push(intent.mutation_id);
                if recovered.has_coordinator_record {
                    current_or_dual_coordinator.push(intent.mutation_id);
                }
            } else if recovered.has_coordinator_record && Some(destination.as_str()) == current_config_etag {
                current_coordinator_only.push(intent.mutation_id);
                current_or_dual_coordinator.push(intent.mutation_id);
            } else if recovered.has_coordinator_record {
                stale_coordinator_only.push(intent.mutation_id);
            }
        }
        current_coordinator_only.sort_unstable();
        peer_backed.sort_unstable();
        let mut replay_order = current_coordinator_only;
        replay_order.extend(peer_backed);

        stale_coordinator_only.sort_unstable();
        current_or_dual_coordinator.sort_unstable();
        let mut coordinator_cleanup_order = stale_coordinator_only;
        coordinator_cleanup_order.extend(current_or_dual_coordinator);
        peer_cleanup_order.sort_unstable();
        Ok(CommittedMutationRecoveryPlan {
            replay_order,
            coordinator_cleanup_order,
            peer_cleanup_order,
        })
    }

    async fn replay_recovered_committed_mutation_intents(
        snapshot: &TierMutationRecoverySnapshot,
        recovery_plan: &CommittedMutationRecoveryPlan,
    ) -> io::Result<()> {
        let intents_by_id = snapshot
            .committed_intents()
            .map(|recovered| (recovered.intent.mutation_id, recovered))
            .collect::<HashMap<_, _>>();
        let mut intents = Vec::with_capacity(recovery_plan.replay_order.len());
        for mutation_id in &recovery_plan.replay_order {
            let recovered = intents_by_id
                .get(mutation_id)
                .ok_or_else(|| io::Error::other("committed tier mutation replay plan references a missing intent"))?;
            intents.push(&recovered.intent);
        }
        Self::replay_committed_mutation_intent_refs(&intents).await
    }

    #[cfg(test)]
    async fn replay_committed_mutation_intents(intents: &[TierMutationIntent]) -> io::Result<()> {
        let intents = intents
            .iter()
            .filter(|intent| intent.state == TierMutationIntentState::Committed)
            .collect::<Vec<_>>();
        Self::replay_committed_mutation_intent_refs(&intents).await
    }

    async fn replay_committed_mutation_intent_refs(intents: &[&TierMutationIntent]) -> io::Result<()> {
        if intents.is_empty() {
            return Ok(());
        }
        let peers = remote_tier_mutation_peers().await?;
        if peers.is_empty() {
            return Ok(());
        }
        for intent in intents {
            let committed_config_etag = intent
                .committed_config_etag
                .as_deref()
                .ok_or_else(|| io::Error::other("committed tier mutation intent is missing committed config etag"))?;
            commit_tier_mutation_peers(intent.mutation_id, peers.clone(), committed_config_etag).await?;
        }
        Ok(())
    }

    async fn delete_finished_peer_mutation_intents<S>(
        api: Arc<S>,
        snapshot: &TierMutationRecoverySnapshot,
        _peer_cleanup_order: &[uuid::Uuid],
    ) -> io::Result<()>
    where
        S: EcstoreObjectIO + EcstoreObjectOperations,
    {
        let now = i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos()).unwrap_or(i64::MAX);
        let mut mutation_ids = snapshot
            .intents
            .iter()
            .filter(|recovered| {
                recovered.has_peer_record
                    && recovered.intent.state != TierMutationIntentState::Prepared
                    && !snapshot.allowance.mutation_ids.contains(&recovered.intent.mutation_id)
            })
            .map(|recovered| recovered.intent.mutation_id)
            .collect::<Vec<_>>();
        mutation_ids.sort_unstable();
        mutation_ids.dedup();
        for mutation_id in mutation_ids {
            let _mutation_guard = acquire_tier_mutation_mutex(mutation_id).await;
            Self::gc_peer_terminal_mutation_intent(api.clone(), mutation_id, now).await?;
        }
        Ok(())
    }

    async fn delete_finished_coordinator_mutation_intents<S>(
        api: Arc<S>,
        snapshot: &TierMutationRecoverySnapshot,
        committed_cleanup_order: &[uuid::Uuid],
    ) -> io::Result<()>
    where
        S: EcstoreObjectIO + EcstoreObjectOperations,
    {
        for mutation_id in committed_cleanup_order {
            let _mutation_guard = acquire_tier_mutation_mutex(*mutation_id).await;
            let expected = snapshot
                .coordinator_intents
                .iter()
                .find(|intent| intent.mutation_id == *mutation_id)
                .ok_or_else(|| io::Error::other("coordinator cleanup plan references a missing intent"))?;
            if !Self::delete_exact_coordinator_terminal_mutation_intent(api.clone(), expected).await? {
                return Err(stable_io_error(
                    "coordinator tier mutation intent changed during conditional cleanup",
                    mutation_id.to_string(),
                ));
            }
        }
        let mut aborted = snapshot
            .coordinator_intents
            .iter()
            .filter(|intent| intent.state == TierMutationIntentState::Aborted)
            .map(|intent| intent.mutation_id)
            .collect::<Vec<_>>();
        aborted.sort_unstable();
        for mutation_id in aborted {
            let _mutation_guard = acquire_tier_mutation_mutex(mutation_id).await;
            let expected = snapshot
                .coordinator_intents
                .iter()
                .find(|intent| intent.mutation_id == mutation_id)
                .ok_or_else(|| io::Error::other("aborted coordinator cleanup lost its source intent"))?;
            if !Self::delete_exact_coordinator_terminal_mutation_intent(api.clone(), expected).await? {
                return Err(stable_io_error(
                    "aborted coordinator tier mutation intent changed during conditional cleanup",
                    mutation_id.to_string(),
                ));
            }
        }
        Ok(())
    }

    #[cfg(test)]
    async fn reconcile_prepared_mutation_intents(
        handle: &Arc<RwLock<Self>>,
        intents: &[TierMutationIntent],
    ) -> std::result::Result<(), AdminError> {
        if Self::reconcile_prepared_mutation_intents_with_revision(handle, intents, None).await? {
            return Ok(());
        }
        let mut err = ERR_TIER_INVALID_CONFIG.clone();
        err.message = "Remote tier mutation block recovery changed repeatedly".to_string();
        Err(err)
    }

    #[cfg(test)]
    async fn reconcile_prepared_mutation_intents_from_store<S>(handle: &Arc<RwLock<Self>>, api: Arc<S>) -> io::Result<()>
    where
        S: EcstoreObjectIO + ListOperations<Error = Error, ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>>,
    {
        Self::load_and_reconcile_mutation_recovery_snapshot(handle, api, true)
            .await
            .map(|_| ())
    }

    #[cfg(test)]
    async fn prepared_mutation_blocks_revision(handle: &Arc<RwLock<Self>>) -> u64 {
        Self::mutation_blocks_revision(handle).await
    }

    async fn mutation_blocks_revision(handle: &Arc<RwLock<Self>>) -> u64 {
        let manager = handle.read().await;
        let runtime = tier_driver_runtime(handle, &manager);
        lock_unpoisoned(&runtime).mutation_blocks_revision
    }

    #[cfg(test)]
    async fn reconcile_prepared_mutation_intents_with_revision(
        handle: &Arc<RwLock<Self>>,
        intents: &[TierMutationIntent],
        base_revision: Option<u64>,
    ) -> std::result::Result<bool, AdminError> {
        let recovered = intents
            .iter()
            .cloned()
            .map(|intent| {
                let state = intent.state;
                RecoveredTierMutationIntent {
                    intent,
                    has_peer_record: true,
                    has_coordinator_record: false,
                    peer_state: Some(state),
                    coordinator_state: None,
                }
            })
            .collect::<Vec<_>>();
        for _ in 0..if base_revision.is_some() { 1 } else { 3 } {
            let expected_revision = match base_revision {
                Some(revision) => revision,
                None => Self::mutation_blocks_revision(handle).await,
            };
            if Self::reconcile_mutation_intent_blocks_with_revision(handle, &recovered, expected_revision, false)
                .await?
                .is_some()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn reconcile_mutation_intent_blocks_with_revision(
        handle: &Arc<RwLock<Self>>,
        intents: &[RecoveredTierMutationIntent],
        expected_revision: u64,
        retain_missing_mutation_blocks: bool,
    ) -> std::result::Result<Option<(MutationBlockAllowance, bool)>, AdminError> {
        let manager = handle.read().await;
        let published_digest = if intents
            .iter()
            .any(|recovered| recovered.intent.state == TierMutationIntentState::Committed)
        {
            Some(tier_config_candidate_digest(&manager).map_err(|err| {
                let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                admin_err.message = format!("Failed to classify retained tier mutation tombstones: {err}");
                admin_err
            })?)
        } else {
            None
        };
        let locally_published_committed_mutations = intents
            .iter()
            .filter(|recovered| {
                recovered.intent.state == TierMutationIntentState::Committed
                    && published_digest == Some(recovered.intent.candidate_digest)
            })
            .map(|recovered| recovered.intent.mutation_id)
            .collect::<HashSet<_>>();
        let mut prepared_mutation_blocks = HashMap::new();
        let mut committed_mutation_blocks: HashMap<String, HashSet<uuid::Uuid>> = HashMap::new();
        for recovered in intents {
            // A matching in-memory manager has already crossed the local
            // publication boundary. Keep replaying and durably cleaning the
            // record, but do not re-fence object operations while that
            // terminal work finishes.
            let skip_runtime_fence = locally_published_committed_mutations.contains(&recovered.intent.mutation_id)
                || (recovered.is_peer_only_terminal()
                    && match recovered.intent.state {
                        TierMutationIntentState::Aborted => true,
                        TierMutationIntentState::Committed => !retain_missing_mutation_blocks,
                        TierMutationIntentState::Prepared => false,
                    });
            if skip_runtime_fence {
                continue;
            }
            Self::collect_prepared_mutation_intent_block(&mut prepared_mutation_blocks, &recovered.intent)?;
            Self::collect_committed_mutation_intent_blocks(&mut committed_mutation_blocks, &recovered.intent);
        }
        let runtime = tier_driver_runtime(handle, &manager);
        let mut runtime = lock_unpoisoned(&runtime);
        if runtime.mutation_blocks_revision != expected_revision {
            return Ok(None);
        }
        if retain_missing_mutation_blocks {
            for (tier_name, mutation_id) in &runtime.prepared_mutation_blocks {
                if locally_published_committed_mutations.contains(mutation_id) {
                    continue;
                }
                match prepared_mutation_blocks.entry(tier_name.clone()) {
                    Entry::Vacant(entry) => {
                        entry.insert(*mutation_id);
                    }
                    Entry::Occupied(entry) if *entry.get() == *mutation_id => {}
                    Entry::Occupied(_) => {
                        let mut err = ERR_TIER_BACKEND_IN_USE.clone();
                        err.message = format!("Remote tier {tier_name} has conflicting prepared mutations during recovery");
                        return Err(err);
                    }
                }
            }
            for (tier_name, mutation_ids) in &runtime.committed_mutation_blocks {
                for mutation_id in mutation_ids {
                    if locally_published_committed_mutations.contains(mutation_id) {
                        continue;
                    }
                    committed_mutation_blocks
                        .entry(tier_name.clone())
                        .or_default()
                        .insert(*mutation_id);
                }
            }
        }
        let changed = runtime.prepared_mutation_blocks != prepared_mutation_blocks
            || runtime.committed_mutation_blocks != committed_mutation_blocks;
        if changed {
            let next_revision = runtime.mutation_blocks_revision.checked_add(1).ok_or_else(|| {
                let mut err = ERR_TIER_INVALID_CONFIG.clone();
                err.message = "Remote tier mutation block revision exhausted".to_string();
                err
            })?;
            runtime.prepared_mutation_blocks = prepared_mutation_blocks;
            runtime.committed_mutation_blocks = committed_mutation_blocks;
            runtime.mutation_blocks_revision = next_revision;
        }
        let mutation_ids = runtime
            .prepared_mutation_blocks
            .values()
            .copied()
            .chain(
                runtime
                    .committed_mutation_blocks
                    .values()
                    .flat_map(|mutation_ids| mutation_ids.iter().copied()),
            )
            .collect();
        Ok(Some((
            MutationBlockAllowance {
                mutation_ids,
                revision: runtime.mutation_blocks_revision,
            },
            !runtime.committed_mutation_blocks.is_empty(),
        )))
    }

    pub(crate) async fn apply_prepared_mutation_intent_block(
        handle: &Arc<RwLock<Self>>,
        intent: &TierMutationIntent,
    ) -> std::result::Result<(), AdminError> {
        let target_tiers = intent
            .affected_targets
            .iter()
            .map(|target| target.tier_name.clone())
            .collect();
        Self::apply_prepared_mutation_intent_block_for_tiers(handle, intent, &target_tiers).await
    }

    async fn apply_prepared_mutation_intent_block_for_tiers(
        handle: &Arc<RwLock<Self>>,
        intent: &TierMutationIntent,
        target_tiers: &HashSet<String>,
    ) -> std::result::Result<(), AdminError> {
        if intent.state != TierMutationIntentState::Prepared {
            return Ok(());
        }
        let manager = handle.read().await;
        let runtime = tier_driver_runtime(handle, &manager);
        let mut runtime = lock_unpoisoned(&runtime);
        let mut prepared_mutation_blocks = runtime.prepared_mutation_blocks.clone();
        Self::collect_prepared_mutation_intent_block(&mut prepared_mutation_blocks, intent)?;
        for tier_name in target_tiers {
            match prepared_mutation_blocks.entry(tier_name.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(intent.mutation_id);
                }
                Entry::Occupied(entry) if *entry.get() == intent.mutation_id => {}
                Entry::Occupied(_) => {
                    let mut err = ERR_TIER_BACKEND_IN_USE.clone();
                    err.message = format!("Remote tier {tier_name} already has another prepared mutation");
                    return Err(err);
                }
            }
        }
        if prepared_mutation_blocks == runtime.prepared_mutation_blocks {
            return Ok(());
        }
        runtime.mutation_blocks_revision = runtime.mutation_blocks_revision.checked_add(1).ok_or_else(|| {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier mutation block revision exhausted".to_string();
            err
        })?;
        runtime.prepared_mutation_blocks = prepared_mutation_blocks;
        Ok(())
    }

    pub(crate) async fn wait_for_blocked_tier_operation_leases(
        handle: &Arc<RwLock<Self>>,
        intent: &TierMutationIntent,
    ) -> std::result::Result<(), AdminError> {
        let target_tiers = intent
            .affected_targets
            .iter()
            .map(|target| target.tier_name.clone())
            .collect();
        Self::wait_for_blocked_tier_operation_leases_for_tiers(handle, &target_tiers).await
    }

    async fn wait_for_blocked_tier_operation_leases_for_tiers(
        handle: &Arc<RwLock<Self>>,
        target_tiers: &HashSet<String>,
    ) -> std::result::Result<(), AdminError> {
        let generations = {
            let manager = handle.read().await;
            let Some(runtime) = registered_tier_driver_runtime(&manager) else {
                return Ok(());
            };
            let runtime = lock_unpoisoned(&runtime);
            target_tiers
                .iter()
                .filter_map(|tier_name| runtime.generations.get(tier_name).cloned())
                .collect::<Vec<_>>()
        };
        let drain = async {
            for generation in generations {
                generation.wait_for_no_active_leases().await;
            }
        };
        if tokio::time::timeout(TIER_OPERATION_DRAIN_TIMEOUT, drain).await.is_err() {
            let mut err = ERR_TIER_BACKEND_IN_USE.clone();
            err.message = "Timed out waiting for in-flight remote tier operations before reference proof".to_string();
            return Err(err);
        }
        Ok(())
    }

    pub(crate) async fn clear_prepared_mutation_intent_block(
        handle: &Arc<RwLock<Self>>,
        mutation_id: uuid::Uuid,
    ) -> std::result::Result<(), AdminError> {
        let manager = handle.read().await;
        let Some(runtime) = registered_tier_driver_runtime(&manager) else {
            return Ok(());
        };
        let mut runtime = lock_unpoisoned(&runtime);
        let changed = runtime
            .prepared_mutation_blocks
            .values()
            .any(|blocked_mutation_id| *blocked_mutation_id == mutation_id);
        if !changed {
            return Ok(());
        }
        runtime.mutation_blocks_revision = runtime.mutation_blocks_revision.checked_add(1).ok_or_else(|| {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier mutation block revision exhausted".to_string();
            err
        })?;
        runtime
            .prepared_mutation_blocks
            .retain(|_, blocked_mutation_id| *blocked_mutation_id != mutation_id);
        Ok(())
    }

    async fn clear_committed_mutation_intent_block(
        handle: &Arc<RwLock<Self>>,
        mutation_id: uuid::Uuid,
    ) -> std::result::Result<(), AdminError> {
        let manager = handle.read().await;
        let Some(runtime) = registered_tier_driver_runtime(&manager) else {
            return Ok(());
        };
        let mut runtime = lock_unpoisoned(&runtime);
        let changed = runtime
            .committed_mutation_blocks
            .values()
            .any(|mutation_ids| mutation_ids.contains(&mutation_id));
        if !changed {
            return Ok(());
        }
        runtime.mutation_blocks_revision = runtime.mutation_blocks_revision.checked_add(1).ok_or_else(|| {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier mutation block revision exhausted".to_string();
            err
        })?;
        runtime.committed_mutation_blocks.retain(|_, mutation_ids| {
            mutation_ids.remove(&mutation_id);
            !mutation_ids.is_empty()
        });
        Ok(())
    }

    pub(crate) async fn promote_prepared_mutation_intent_block(
        handle: &Arc<RwLock<Self>>,
        mutation_id: uuid::Uuid,
    ) -> std::result::Result<(), AdminError> {
        let manager = handle.read().await;
        let runtime = tier_driver_runtime(handle, &manager);
        let mut runtime = lock_unpoisoned(&runtime);
        let tiers = runtime
            .prepared_mutation_blocks
            .iter()
            .filter(|(_, blocked_mutation_id)| **blocked_mutation_id == mutation_id)
            .map(|(tier_name, _)| tier_name.clone())
            .collect::<Vec<_>>();
        if tiers.is_empty() {
            runtime.mutation_refresh.notify_one();
            return Ok(());
        }
        let next_revision = runtime.mutation_blocks_revision.checked_add(1).ok_or_else(|| {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier mutation block revision exhausted".to_string();
            err
        })?;
        for tier_name in tiers {
            runtime.prepared_mutation_blocks.remove(&tier_name);
            runtime
                .committed_mutation_blocks
                .entry(tier_name)
                .or_default()
                .insert(mutation_id);
        }
        runtime.mutation_blocks_revision = next_revision;
        runtime.mutation_refresh.notify_one();
        Ok(())
    }

    pub(crate) async fn apply_committed_mutation_intent_block(
        handle: &Arc<RwLock<Self>>,
        intent: &TierMutationIntent,
    ) -> std::result::Result<(), AdminError> {
        if intent.state != TierMutationIntentState::Committed {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier committed mutation block has a non-committed intent".to_string();
            return Err(err);
        }
        let manager = handle.read().await;
        let runtime = tier_driver_runtime(handle, &manager);
        let mut runtime = lock_unpoisoned(&runtime);
        let mutation_refresh = runtime.mutation_refresh.clone();
        let changed = runtime
            .prepared_mutation_blocks
            .values()
            .any(|blocked_mutation_id| *blocked_mutation_id == intent.mutation_id)
            || intent.affected_targets.iter().any(|target| {
                !runtime
                    .committed_mutation_blocks
                    .get(&target.tier_name)
                    .is_some_and(|mutation_ids| mutation_ids.contains(&intent.mutation_id))
            });
        if !changed {
            drop(runtime);
            drop(manager);
            mutation_refresh.notify_one();
            return Ok(());
        }
        let next_revision = runtime.mutation_blocks_revision.checked_add(1).ok_or_else(|| {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier mutation block revision exhausted".to_string();
            err
        })?;
        runtime
            .prepared_mutation_blocks
            .retain(|_, blocked_mutation_id| *blocked_mutation_id != intent.mutation_id);
        for target in &intent.affected_targets {
            runtime
                .committed_mutation_blocks
                .entry(target.tier_name.clone())
                .or_default()
                .insert(intent.mutation_id);
        }
        runtime.mutation_blocks_revision = next_revision;
        drop(runtime);
        drop(manager);
        mutation_refresh.notify_one();
        Ok(())
    }

    fn collect_prepared_mutation_intent_block(
        prepared_mutation_blocks: &mut HashMap<String, uuid::Uuid>,
        intent: &TierMutationIntent,
    ) -> std::result::Result<(), AdminError> {
        if intent.state != TierMutationIntentState::Prepared {
            return Ok(());
        }
        for target in &intent.affected_targets {
            match prepared_mutation_blocks.entry(target.tier_name.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(intent.mutation_id);
                }
                Entry::Occupied(entry) if *entry.get() == intent.mutation_id => {}
                Entry::Occupied(_) => {
                    let mut err = ERR_TIER_BACKEND_IN_USE.clone();
                    err.message = format!("Remote tier {} already has another prepared mutation", target.tier_name);
                    return Err(err);
                }
            }
        }
        Ok(())
    }

    fn collect_committed_mutation_intent_blocks(
        committed_mutation_blocks: &mut HashMap<String, HashSet<uuid::Uuid>>,
        intent: &TierMutationIntent,
    ) {
        if intent.state != TierMutationIntentState::Committed {
            return;
        }
        for target in &intent.affected_targets {
            committed_mutation_blocks
                .entry(target.tier_name.clone())
                .or_default()
                .insert(intent.mutation_id);
        }
    }

    pub async fn add_and_save(
        handle: &Arc<RwLock<Self>>,
        api: Arc<ECStore>,
        tier_config: TierConfig,
        force: bool,
    ) -> std::result::Result<(), TierConfigUpdateError> {
        let mutation = TierCandidateMutation::add(tier_config, force).map_err(TierConfigUpdateError::Mutation)?;
        Self::update_candidate_with_config_lock(handle, api, mutation).await
    }

    pub async fn edit_and_save(
        handle: &Arc<RwLock<Self>>,
        api: Arc<ECStore>,
        tier_name: &str,
        credentials: TierCreds,
    ) -> std::result::Result<(), TierConfigUpdateError> {
        Self::update_candidate_with_config_lock(handle, api, TierCandidateMutation::Edit(tier_name.to_string(), credentials))
            .await
    }

    #[cfg(test)]
    async fn edit_and_save_with<S>(
        handle: &Arc<RwLock<Self>>,
        api: Arc<S>,
        tier_name: &str,
        credentials: TierCreds,
    ) -> std::result::Result<(), TierConfigUpdateError>
    where
        S: TierReferenceProofStore + 'static,
    {
        let update = Self::admin_update_lock(handle).await;
        let (candidate, version) = load_tier_config_for_update(api.clone())
            .await
            .map_err(TierConfigUpdateError::Load)?;
        Self::update_candidate_owned(
            handle,
            api,
            candidate,
            version,
            TierCandidateMutation::Edit(tier_name.to_string(), credentials),
            update,
            None,
        )
        .await
    }

    pub async fn remove_and_save(
        handle: &Arc<RwLock<Self>>,
        api: Arc<ECStore>,
        tier_name: &str,
        force: bool,
    ) -> std::result::Result<(), TierConfigUpdateError> {
        Self::update_candidate_with_config_lock(handle, api, TierCandidateMutation::Remove(tier_name.to_string(), force)).await
    }

    #[allow(dead_code, reason = "reached only through #[cfg(test)] helpers in this file (backlog#1823)")]
    async fn remove_and_save_with<S>(
        handle: &Arc<RwLock<Self>>,
        api: Arc<S>,
        tier_name: &str,
        force: bool,
    ) -> std::result::Result<(), TierConfigUpdateError>
    where
        S: TierReferenceProofStore + 'static,
    {
        let update = Self::admin_update_lock(handle).await;
        let (candidate, version) = load_tier_config_for_update(api.clone())
            .await
            .map_err(TierConfigUpdateError::Load)?;
        Self::update_candidate_owned(
            handle,
            api,
            candidate,
            version,
            TierCandidateMutation::Remove(tier_name.to_string(), force),
            update,
            None,
        )
        .await
    }

    pub async fn clear_and_save(
        handle: &Arc<RwLock<Self>>,
        api: Arc<ECStore>,
        force: bool,
    ) -> std::result::Result<(), TierConfigUpdateError> {
        Self::update_candidate_with_config_lock(handle, api, TierCandidateMutation::Clear(force)).await
    }

    #[allow(dead_code, reason = "reached only through #[cfg(test)] helpers in this file (backlog#1823)")]
    async fn clear_and_save_with<S>(
        handle: &Arc<RwLock<Self>>,
        api: Arc<S>,
        force: bool,
    ) -> std::result::Result<(), TierConfigUpdateError>
    where
        S: TierReferenceProofStore + 'static,
    {
        let update = Self::admin_update_lock(handle).await;
        let (candidate, version) = load_tier_config_for_update(api.clone())
            .await
            .map_err(TierConfigUpdateError::Load)?;
        Self::update_candidate_owned(handle, api, candidate, version, TierCandidateMutation::Clear(force), update, None).await
    }

    pub async fn verify_without_manager_lock(handle: &Arc<RwLock<Self>>, tier_name: &str) -> std::result::Result<(), io::Error> {
        let lease = Self::acquire_operation_lease(handle, tier_name)
            .await
            .map_err(io::Error::other)?;
        tokio::spawn(async move {
            let driver: WarmBackendImpl = Box::new(SharedWarmBackendProxy(lease.inner.driver.clone()));
            let result = check_warm_backend(Some(&driver)).await.map_err(io::Error::other);
            drop(lease);
            result
        })
        .await
        .map_err(|_| io::Error::other("remote tier verification task failed"))?
    }

    pub(crate) async fn acquire_operation_lease_for_backend_identity(
        handle: &Arc<RwLock<Self>>,
        tier_name: &str,
        expected: TierDestinationId,
    ) -> std::result::Result<TierOperationLease, AdminError> {
        let lease = Self::acquire_operation_lease(handle, tier_name).await?;
        if lease.backend_identity() != expected {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier backend identity no longer matches the recorded operation".to_string();
            return Err(err);
        }
        Ok(lease)
    }

    #[cfg(test)]
    #[allow(
        dead_code,
        reason = "lease accounting asserted by a bucket_lifecycle_ops test behind `--features test-util` (backlog#1823)"
    )]
    pub(crate) async fn active_operation_lease_count(handle: &Arc<RwLock<Self>>, tier_name: &str) -> usize {
        let manager = handle.read().await;
        let Some(runtime) = registered_tier_driver_runtime(&manager) else {
            return 0;
        };
        lock_unpoisoned(&runtime)
            .generations
            .get(tier_name)
            .map(|generation| generation.active_leases.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    fn replace_driver(&mut self, tier_name: &str, driver: WarmBackendImpl) -> std::result::Result<(), AdminError> {
        let Some(runtime) = registered_tier_driver_runtime(self) else {
            self.driver_cache.insert(tier_name.to_string(), driver);
            return Ok(());
        };
        let config = self.tiers.get(tier_name).ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?;
        let config_fingerprint = tier_config_fingerprint(tier_name, config).map_err(|err| {
            let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
            admin_err.message = err.to_string();
            admin_err
        })?;
        let backend_identity = tier_backend_identity(config).map_err(|err| {
            let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
            admin_err.message = err.to_string();
            admin_err
        })?;
        let exact_get_delete = tier_exact_get_delete(config);
        let mut runtime_guard = lock_unpoisoned(&runtime);
        let generation = runtime_guard.next_generation.checked_add(1).ok_or_else(|| {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier driver generation exhausted".to_string();
            err
        })?;
        let driver: SharedWarmBackend = Arc::from(driver);
        let entry = Arc::new(TierDriverGeneration {
            tier_name: Arc::from(tier_name),
            tier_config: config.clone_with_credentials(),
            generation,
            config_fingerprint,
            backend_identity,
            exact_get_delete,
            driver: driver.clone(),
            reconciler: tokio::sync::OnceCell::new(),
            accepting: AtomicBool::new(true),
            active_leases: AtomicUsize::new(0),
            drained: tokio::sync::Notify::new(),
        });

        if let Some(previous) = runtime_guard.generations.insert(tier_name.to_string(), entry) {
            previous.accepting.store(false, Ordering::Release);
        }
        runtime_guard.next_generation = generation;
        drop(runtime_guard);
        self.driver_cache
            .insert(tier_name.to_string(), Box::new(SharedWarmBackendProxy(driver)));
        Ok(())
    }

    #[cfg(any(test, feature = "test-util"))]
    pub(crate) fn install_test_driver(
        &mut self,
        tier_name: &str,
        driver: WarmBackendImpl,
    ) -> std::result::Result<(), AdminError> {
        self.replace_driver(tier_name, driver)?;
        if let Some(runtime) = registered_tier_driver_runtime(self)
            && let Some(generation) = lock_unpoisoned(&runtime).generations.get(tier_name).cloned()
        {
            // Test drivers own their candidate inventory; do not reconstruct a
            // real network reconciler from the placeholder test configuration.
            generation.reconciler.set(None).map_err(|_| {
                let mut err = ERR_TIER_INVALID_CONFIG.clone();
                err.message = "Test tier candidate reconciler was already initialized".to_string();
                err
            })?;
        }
        Ok(())
    }

    fn revoke_driver(&mut self, tier_name: &str) -> Option<Arc<TierDriverGeneration>> {
        let generation =
            registered_tier_driver_runtime(self).and_then(|runtime| lock_unpoisoned(&runtime).generations.remove(tier_name));
        if let Some(generation) = generation.as_ref() {
            generation.accepting.store(false, Ordering::Release);
        }
        self.driver_cache.remove(tier_name);
        generation
    }

    fn ensure_generation_is_idle(&self, tier_name: &str) -> std::result::Result<(), AdminError> {
        let Some(runtime) = registered_tier_driver_runtime(self) else {
            return Ok(());
        };
        let runtime = lock_unpoisoned(&runtime);
        let blocked = runtime_blocks_tier(&runtime, tier_name)
            || runtime
                .generations
                .get(tier_name)
                .is_some_and(|generation| generation.active_leases.load(Ordering::Acquire) != 0);
        if blocked {
            return Err(ERR_TIER_BACKEND_IN_USE.clone());
        }
        Ok(())
    }

    fn ensure_generations_are_idle<'a>(
        &self,
        tier_names: impl IntoIterator<Item = &'a String>,
    ) -> std::result::Result<(), AdminError> {
        for tier_name in tier_names {
            self.ensure_generation_is_idle(tier_name)?;
        }
        Ok(())
    }

    fn revoke_all_drivers(&mut self) {
        if let Some(runtime) = registered_tier_driver_runtime(self) {
            let mut runtime = lock_unpoisoned(&runtime);
            for (_, generation) in runtime.generations.drain() {
                generation.accepting.store(false, Ordering::Release);
            }
        }
        self.driver_cache.clear();
    }

    fn retire_all_drivers(&mut self) {
        self.revoke_all_drivers();
    }

    pub async fn reload(&mut self, api: Arc<ECStore>) -> std::result::Result<(), std::io::Error> {
        let config = load_tier_config(api).await?;
        self.publish_legacy_reload(config).await?;
        Ok(())
    }

    pub(crate) async fn publish_legacy_reload(&mut self, config: Self) -> io::Result<()> {
        let changed = changed_tier_names(self, &config);
        let replaced_destinations = replaced_tier_destinations(self, &config).map_err(io::Error::other)?;
        if let Some(runtime) = registered_tier_driver_runtime(self) {
            for tier_name in replaced_destinations.keys() {
                if !lock_unpoisoned(&runtime).generations.contains_key(tier_name)
                    && let Some(driver) = self.driver_cache.remove(tier_name)
                {
                    self.replace_driver(tier_name, driver).map_err(io::Error::other)?;
                }
            }
            let mut transition = Self::begin_tier_transition_in_runtime(runtime, changed, replaced_destinations, None)
                .map_err(io::Error::other)?;
            transition.wait_for_active_leases().await.map_err(io::Error::other)?;
            transition
                .ensure_replaced_destinations_are_empty()
                .await
                .map_err(io::Error::other)?;
            let runtime = lock_unpoisoned(&transition.runtime);
            if !transition
                .tokens
                .iter()
                .all(|(tier_name, epoch)| runtime.draining.get(tier_name) == Some(epoch))
            {
                return Err(io::Error::other("remote tier replacement ownership was lost before reload publish"));
            }
            if transition
                .tokens
                .iter()
                .any(|(tier_name, _)| runtime_mutation_blocks_tier_transition(&runtime, tier_name, None))
            {
                return Err(io::Error::other("remote tier mutation recovery changed before reload publish"));
            }
            for (tier_name, _) in &transition.tokens {
                self.driver_cache.remove(tier_name);
            }
            self.tiers = config.tiers;
            drop(runtime);
            transition.published = true;
        } else {
            ensure_replaced_destinations_are_empty(&replaced_destinations, &HashMap::new())
                .await
                .map_err(io::Error::other)?;
            self.tiers = config.tiers;
        }
        self.last_refreshed_at = OffsetDateTime::now_utc();
        Ok(())
    }

    fn apply_reloaded_tiers(&mut self, tiers: HashMap<String, TierConfig>) -> std::result::Result<(), AdminError> {
        if registered_tier_driver_runtime(self).is_some_and(|runtime| runtime_has_mutation_block(&lock_unpoisoned(&runtime))) {
            return Err(ERR_TIER_BACKEND_IN_USE.clone());
        }
        let changed_or_removed = self
            .tiers
            .iter()
            .filter_map(|(tier_name, current)| {
                let unchanged = tiers
                    .get(tier_name)
                    .is_some_and(|replacement| tier_configs_match(tier_name, current, replacement));
                (!unchanged).then(|| tier_name.clone())
            })
            .collect::<Vec<_>>();
        self.ensure_generations_are_idle(&changed_or_removed)?;
        for tier_name in &changed_or_removed {
            self.revoke_driver(tier_name);
        }
        self.tiers = tiers;
        Ok(())
    }

    pub async fn rollback_after_failed_save(&mut self, api: Arc<ECStore>) -> io::Result<()> {
        match load_tier_config(api).await {
            Ok(config) => {
                self.apply_reloaded_tiers(config.tiers).map_err(io::Error::other)?;
                Ok(())
            }
            Err(err) => {
                if let Some(runtime) = registered_tier_driver_runtime(self) {
                    let runtime = lock_unpoisoned(&runtime);
                    if runtime_has_mutation_block(&runtime)
                        || runtime
                            .generations
                            .values()
                            .any(|generation| generation.active_leases.load(Ordering::Acquire) != 0)
                    {
                        return Err(io::Error::other(format!(
                            "failed to reload tier configuration for rollback while a tier generation is active: {err}"
                        )));
                    }
                }
                self.retire_all_drivers();
                self.tiers.clear();
                Err(err)
            }
        }
    }

    pub async fn clear_tier(&mut self, force: bool) -> std::result::Result<(), AdminError> {
        if registered_tier_driver_runtime(self).is_some_and(|runtime| runtime_has_mutation_block(&lock_unpoisoned(&runtime))) {
            return Err(ERR_TIER_BACKEND_IN_USE.clone());
        }
        self.ensure_generations_are_idle(self.tiers.keys())?;
        if !force {
            let tier_names = self.tiers.keys().cloned().collect::<Vec<_>>();
            for tier_name in tier_names {
                match self.get_driver(&tier_name).await?.in_use().await {
                    Ok(true) => return Err(ERR_TIER_BACKEND_NOT_EMPTY.clone()),
                    Ok(false) => {}
                    Err(err) => {
                        let mut admin_err = ERR_TIER_PERM_ERR.clone();
                        admin_err.message.push('.');
                        admin_err.message.push_str(&err.to_string());
                        return Err(admin_err);
                    }
                }
            }
        }
        self.tiers.clear();
        self.revoke_all_drivers();
        Ok(())
    }

    #[tracing::instrument(level = "debug", name = "tier_save", skip(self))]
    pub async fn save(&self) -> std::result::Result<(), std::io::Error> {
        let Some(api) = runtime_sources::object_store_handle() else {
            return Err(tier_config_not_initialized_error("save tiering config"));
        };

        self.save_tiering_config(api).await
    }

    pub async fn save_tiering_config<S>(&self, api: Arc<S>) -> std::result::Result<(), std::io::Error>
    where
        S: ObjectIO<
                Error = Error,
                RangeSpec = HTTPRangeSpec,
                HeaderMap = HeaderMap,
                ObjectOptions = ObjectOptions,
                ObjectInfo = ObjectInfo,
                GetObjectReader = GetObjectReader,
                PutObjectReader = PutObjReader,
            >,
    {
        let data = encode_external_tiering_config_blob(self)?;
        let config_file = tier_config_path(TIER_CONFIG_FILE);

        self.save_config(api, &config_file, data).await
    }

    #[allow(dead_code, reason = "reached only through #[cfg(test)] helpers in this file (backlog#1823)")]
    async fn save_tiering_config_if_current<S>(
        &self,
        api: Arc<S>,
        version: Option<&str>,
    ) -> std::result::Result<(), std::io::Error>
    where
        S: ObjectIO<
                Error = Error,
                RangeSpec = HTTPRangeSpec,
                HeaderMap = HeaderMap,
                ObjectOptions = ObjectOptions,
                ObjectInfo = ObjectInfo,
                GetObjectReader = GetObjectReader,
                PutObjectReader = PutObjReader,
            >,
    {
        self.save_tiering_config_if_current_with_info(api, version).await?;
        Ok(())
    }

    async fn save_tiering_config_if_current_with_info<S>(
        &self,
        api: Arc<S>,
        version: Option<&str>,
    ) -> std::result::Result<ObjectInfo, std::io::Error>
    where
        S: ObjectIO<
                Error = Error,
                RangeSpec = HTTPRangeSpec,
                HeaderMap = HeaderMap,
                ObjectOptions = ObjectOptions,
                ObjectInfo = ObjectInfo,
                GetObjectReader = GetObjectReader,
                PutObjectReader = PutObjReader,
            >,
    {
        let data = encode_external_tiering_config_blob(self)?;
        let config_file = tier_config_path(TIER_CONFIG_FILE);
        let http_preconditions = match version {
            Some(etag) => HTTPPreconditions {
                if_match: Some(etag.to_string()),
                ..Default::default()
            },
            None => HTTPPreconditions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            },
        };

        self.save_config_with_opts_info(
            api,
            &config_file,
            data,
            &ObjectOptions {
                max_parity: true,
                http_preconditions: Some(http_preconditions),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn save_config<S>(&self, api: Arc<S>, file: &str, data: Bytes) -> std::result::Result<(), std::io::Error>
    where
        S: ObjectIO<
                Error = Error,
                RangeSpec = HTTPRangeSpec,
                HeaderMap = HeaderMap,
                ObjectOptions = ObjectOptions,
                ObjectInfo = ObjectInfo,
                GetObjectReader = GetObjectReader,
                PutObjectReader = PutObjReader,
            >,
    {
        self.save_config_with_opts(
            api,
            file,
            data,
            &ObjectOptions {
                max_parity: true,
                ..Default::default()
            },
        )
        .await
    }

    pub async fn save_config_with_opts<S>(
        &self,
        api: Arc<S>,
        file: &str,
        data: Bytes,
        opts: &ObjectOptions,
    ) -> std::result::Result<(), std::io::Error>
    where
        S: ObjectIO<
                Error = Error,
                RangeSpec = HTTPRangeSpec,
                HeaderMap = HeaderMap,
                ObjectOptions = ObjectOptions,
                ObjectInfo = ObjectInfo,
                GetObjectReader = GetObjectReader,
                PutObjectReader = PutObjReader,
            >,
    {
        self.save_config_with_opts_info(api, file, data, opts).await?;
        Ok(())
    }

    async fn save_config_with_opts_info<S>(
        &self,
        api: Arc<S>,
        file: &str,
        data: Bytes,
        opts: &ObjectOptions,
    ) -> std::result::Result<ObjectInfo, std::io::Error>
    where
        S: ObjectIO<
                Error = Error,
                RangeSpec = HTTPRangeSpec,
                HeaderMap = HeaderMap,
                ObjectOptions = ObjectOptions,
                ObjectInfo = ObjectInfo,
                GetObjectReader = GetObjectReader,
                PutObjectReader = PutObjReader,
            >,
    {
        debug!("save tier config:{}", file);
        let mut put_data = PutObjReader::from_vec(data.to_vec());
        api.put_object(RUSTFS_META_BUCKET, file, &mut put_data, opts)
            .await
            .map_err(io::Error::other)
    }

    pub async fn refresh_tier_config(&mut self, api: Arc<ECStore>) {
        let r = rand::rng().random_range(0.0..1.0);
        let rand_interval = || Duration::from_secs((r * 60_f64).round() as u64);

        let mut t = interval(TIER_CFG_REFRESH + rand_interval());
        loop {
            select! {
                _ = t.tick() => {
                    if let Err(err) = self.reload(api.clone()).await {
                      info!("{}", err);
                    }
                }
            }
        }
    }

    pub(crate) async fn refresh_tier_config_handle(handle: Arc<RwLock<Self>>, api: Arc<ECStore>) {
        Self::refresh_tier_config_handle_with(handle, api).await;
    }

    pub(crate) async fn refresh_tier_config_handle_with<S>(handle: Arc<RwLock<Self>>, api: Arc<S>)
    where
        S: EcstoreObjectIO
            + EcstoreObjectOperations
            + NamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>
            + ListOperations<Error = Error, ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>>,
    {
        // The periodic refresh remains the recovery fallback; committed mutations
        // notify this worker so a successful peer commit converges immediately.
        let mutation_refresh = Self::mutation_refresh_notifier(&handle).await;
        let r = rand::rng().random_range(0.0..1.0);
        let rand_interval = || Duration::from_secs((r * 60_f64).round() as u64);

        let refresh_interval = TIER_CFG_REFRESH + rand_interval();
        let mut t = delayed_tier_refresh_interval(refresh_interval);
        loop {
            select! {
                _ = t.tick() => {
                    if let Err(err) = Self::reload_handle_with(&handle, api.clone()).await {
                        warn!(
                            event = EVENT_TIER_CONFIG_REFRESH,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_TIER,
                            trigger = "periodic",
                            result = "failed",
                            error = ?err,
                            "tier configuration refresh"
                        );
                    }
                }
                _ = mutation_refresh.notified() => {
                    Self::reload_after_committed_mutation(&handle, api.clone()).await;
                }
            }
            t.reset();
        }
    }

    async fn reload_after_committed_mutation<S>(handle: &Arc<RwLock<Self>>, api: Arc<S>)
    where
        S: EcstoreObjectIO
            + EcstoreObjectOperations
            + NamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>
            + ListOperations<Error = Error, ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>>,
    {
        // 'retry_with_backoff' is finite; this path must continue until the
        // durable committed fence is gone, while bounding the retry delay.
        let mut retry_attempt = 0_u32;
        loop {
            match Self::reload_handle_with(handle, api.clone()).await {
                Ok(()) => return,
                Err(err) => {
                    let has_runtime_fence = Self::has_committed_mutation_block(handle).await;
                    let has_cleanup = Self::terminal_mutation_cleanup_requires_retry(
                        Self::has_retryable_terminal_mutation_cleanup(api.clone()).await,
                    );
                    if !has_runtime_fence && !has_cleanup {
                        warn!(
                            event = EVENT_TIER_CONFIG_REFRESH,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_TIER,
                            trigger = "committed_mutation",
                            result = "failed_without_retryable_state",
                            error = ?err,
                            "tier configuration refresh"
                        );
                        return;
                    }

                    let delay = tier_mutation_refresh_retry_delay(retry_attempt);
                    let attempt = retry_attempt.saturating_add(1);
                    if attempt == 1 {
                        warn!(
                            event = EVENT_TIER_CONFIG_REFRESH,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_TIER,
                            trigger = "committed_mutation",
                            result = "retrying",
                            retry_attempt = attempt,
                            retry_delay_ms = delay.as_millis(),
                            error = ?err,
                            "tier configuration refresh"
                        );
                    } else if attempt.is_power_of_two() {
                        debug!(
                            event = EVENT_TIER_CONFIG_REFRESH,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_TIER,
                            trigger = "committed_mutation",
                            result = "retrying",
                            retry_attempt = attempt,
                            retry_delay_ms = delay.as_millis(),
                            error = ?err,
                            "tier configuration refresh"
                        );
                    }
                    tokio::time::sleep(delay).await;
                    let has_runtime_fence = Self::has_committed_mutation_block(handle).await;
                    let has_cleanup = Self::terminal_mutation_cleanup_requires_retry(
                        Self::has_retryable_terminal_mutation_cleanup(api.clone()).await,
                    );
                    if !has_runtime_fence && !has_cleanup {
                        return;
                    }
                    retry_attempt = retry_attempt.saturating_add(1);
                }
            }
        }
    }

    pub async fn init(&mut self, api: Arc<ECStore>) -> Result<()> {
        self.reload(api).await?;
        //if globalIsDistErasure {
        //    self.refresh_tier_config(api).await;
        //}
        Ok(())
    }
}

async fn new_and_save_tiering_config<S>(api: Arc<S>) -> Result<TierConfigMgr>
where
    S: EcstoreObjectIO,
{
    let mut cfg = TierConfigMgr {
        driver_cache: HashMap::new(),
        tiers: HashMap::new(),
        last_refreshed_at: OffsetDateTime::now_utc(),
    };
    //lookup_configs(&mut cfg, api.clone()).await;
    cfg.save_tiering_config(api).await?;

    Ok(cfg)
}

#[tracing::instrument(level = "debug", name = "load_tier_config", skip(api))]
async fn load_tier_config<S>(api: Arc<S>) -> std::result::Result<TierConfigMgr, std::io::Error>
where
    S: EcstoreObjectIO,
{
    let config_file = tier_config_path(TIER_CONFIG_FILE);
    match read_config(api.clone(), config_file.as_str()).await {
        Ok(data) => decode_tiering_config_blob(&data),
        Err(err) if is_err_config_not_found(&err) => {
            let legacy_file = tier_config_path(TIER_CONFIG_LEGACY_FILE);
            match read_config(api.clone(), legacy_file.as_str()).await {
                Ok(data) => {
                    let cfg = TierConfigMgr::unmarshal(&data)?;
                    let normalized = encode_external_tiering_config_blob(&cfg)?;
                    let mut put_data = PutObjReader::from_vec(normalized.to_vec());
                    let _ = api
                        .put_object(
                            RUSTFS_META_BUCKET,
                            &config_file,
                            &mut put_data,
                            &ObjectOptions {
                                max_parity: true,
                                ..Default::default()
                            },
                        )
                        .await;
                    Ok(cfg)
                }
                Err(legacy_err) if is_err_config_not_found(&legacy_err) => {
                    warn!("config not found, start to init");
                    if runtime_sources::first_cluster_node_is_local().await {
                        new_and_save_tiering_config(api).await.map_err(io::Error::other)
                    } else {
                        Ok(TierConfigMgr {
                            driver_cache: HashMap::new(),
                            tiers: HashMap::new(),
                            last_refreshed_at: OffsetDateTime::now_utc(),
                        })
                    }
                }
                Err(legacy_err) => Err(io::Error::other(legacy_err)),
            }
        }
        Err(err) => {
            error!("read config err {:?}", &err);
            Err(io::Error::other(err))
        }
    }
}

struct LoadedTierConfigForRecovery {
    config: TierConfigMgr,
    etag: Option<String>,
    persisted_digest: Option<TierMutationDigest>,
}

fn loaded_tier_config_matches_candidate_digest(
    loaded: &LoadedTierConfigForRecovery,
    candidate_digest: TierMutationDigest,
) -> io::Result<bool> {
    if loaded.persisted_digest == Some(candidate_digest) {
        return Ok(true);
    }
    Ok(tier_config_candidate_digest(&loaded.config)? == candidate_digest)
}

async fn load_tier_config_for_update<S>(api: Arc<S>) -> std::result::Result<(TierConfigMgr, Option<String>), std::io::Error>
where
    S: EcstoreObjectIO,
{
    let loaded = load_tier_config_for_recovery(api).await?;
    Ok((loaded.config, loaded.etag))
}

async fn load_tier_config_for_recovery<S>(api: Arc<S>) -> std::result::Result<LoadedTierConfigForRecovery, std::io::Error>
where
    S: EcstoreObjectIO,
{
    let config_file = tier_config_path(TIER_CONFIG_FILE);
    match read_config_with_metadata(api.clone(), &config_file, &ObjectOptions::default()).await {
        Ok((data, object_info)) => {
            let etag = object_info
                .etag
                .filter(|etag| !etag.trim().is_empty())
                .ok_or_else(|| io::Error::other("tier configuration object is missing an ETag"))?;
            Ok(LoadedTierConfigForRecovery {
                config: decode_tiering_config_blob(&data)?,
                etag: Some(etag),
                persisted_digest: Some(tier_config_digest(&data)),
            })
        }
        Err(err) if is_err_config_not_found(&err) => {
            let legacy_file = tier_config_path(TIER_CONFIG_LEGACY_FILE);
            match read_config(api, &legacy_file).await {
                Ok(data) => Ok(LoadedTierConfigForRecovery {
                    config: TierConfigMgr::unmarshal(&data)?,
                    etag: None,
                    persisted_digest: Some(tier_config_digest(&data)),
                }),
                Err(legacy_err) if is_err_config_not_found(&legacy_err) => Ok(LoadedTierConfigForRecovery {
                    config: TierConfigMgr {
                        driver_cache: HashMap::new(),
                        tiers: HashMap::new(),
                        last_refreshed_at: OffsetDateTime::now_utc(),
                    },
                    etag: None,
                    persisted_digest: None,
                }),
                Err(legacy_err) => Err(io::Error::other(legacy_err)),
            }
        }
        Err(err) => Err(io::Error::other(err)),
    }
}

pub(crate) async fn tier_config_etag_matches<S>(api: Arc<S>, expected: &str) -> io::Result<bool>
where
    S: EcstoreObjectIO,
{
    let (_, etag) = load_tier_config_for_update(api).await?;
    Ok(etag.as_deref() == Some(expected))
}

pub(crate) async fn tier_config_abort_matches<S>(api: Arc<S>, intent: &TierMutationIntent) -> io::Result<bool>
where
    S: EcstoreObjectIO,
{
    let loaded = load_tier_config_for_recovery(api).await?;
    if let Some(old_config_etag) = intent.old_config_etag.as_deref() {
        return Ok(loaded.etag.as_deref() == Some(old_config_etag));
    }
    Ok(!loaded_tier_config_matches_candidate_digest(&loaded, intent.candidate_digest)?)
}

pub(crate) async fn tier_config_commit_matches<S>(
    api: Arc<S>,
    expected_etag: &str,
    candidate_digest: TierMutationDigest,
) -> io::Result<bool>
where
    S: EcstoreObjectIO,
{
    let loaded = load_tier_config_for_recovery(api).await?;
    if loaded.etag.as_deref() != Some(expected_etag) {
        return Ok(false);
    }
    loaded_tier_config_matches_candidate_digest(&loaded, candidate_digest)
}

async fn read_tier_config_from_bucket<S>(
    api: Arc<S>,
    bucket: &str,
    path: &str,
    opts: &ObjectOptions,
) -> io::Result<Option<Vec<u8>>>
where
    S: EcstoreObjectIO,
{
    let mut rd = match api.get_object_reader(bucket, path, None, HeaderMap::new(), opts).await {
        Ok(v) => v,
        Err(err) if is_err_config_not_found(&err) => return Ok(None),
        Err(err) => return Err(io::Error::other(err)),
    };
    let data = rd.read_all().await.map_err(io::Error::other)?;
    if data.is_empty() {
        return Ok(None);
    }
    Ok(Some(data))
}

async fn write_tier_config_to_rustfs<S>(api: Arc<S>, path: &str, data: Bytes) -> io::Result<()>
where
    S: EcstoreObjectIO,
{
    let mut put_data = PutObjReader::from_vec(data.to_vec());
    api.put_object(
        RUSTFS_META_BUCKET,
        path,
        &mut put_data,
        &ObjectOptions {
            max_parity: true,
            ..Default::default()
        },
    )
    .await
    .map_err(io::Error::other)?;
    Ok(())
}

pub async fn try_migrate_tiering_config<S>(api: Arc<S>)
where
    S: ObjectIO<
            Error = Error,
            RangeSpec = HTTPRangeSpec,
            HeaderMap = HeaderMap,
            ObjectOptions = ObjectOptions,
            ObjectInfo = ObjectInfo,
            GetObjectReader = GetObjectReader,
            PutObjectReader = PutObjReader,
        > + ObjectOperations<
            Error = Error,
            ObjectInfo = ObjectInfo,
            ObjectOptions = ObjectOptions,
            FileInfo = FileInfo,
            ObjectToDelete = ObjectToDelete,
            DeletedObject = DeletedObject,
        >,
{
    let target_path = tier_config_path(TIER_CONFIG_FILE);
    if api
        .get_object_info(
            RUSTFS_META_BUCKET,
            &target_path,
            &ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
        )
        .await
        .is_ok()
    {
        debug!("tier config already exists in RustFS metadata bucket, skip migration");
        return;
    }

    let opts = ObjectOptions {
        max_parity: true,
        no_lock: true,
        ..Default::default()
    };

    let legacy_path = tier_config_path(TIER_CONFIG_LEGACY_FILE);
    match read_tier_config_from_bucket(api.clone(), RUSTFS_META_BUCKET, &legacy_path, &opts).await {
        Ok(Some(data)) => match TierConfigMgr::unmarshal(&data)
            .and_then(|cfg| encode_external_tiering_config_blob(&cfg).map_err(io::Error::other))
        {
            Ok(out) => {
                if write_tier_config_to_rustfs(api.clone(), &target_path, out).await.is_ok() {
                    info!("Migrated tier config from legacy RustFS metadata format");
                    return;
                }
            }
            Err(err) => debug!(
                bucket = RUSTFS_META_BUCKET,
                path = %legacy_path,
                error = %err,
                "Skipping incompatible legacy tier config migration"
            ),
        },
        Ok(None) => {}
        Err(err) => debug!(
            bucket = RUSTFS_META_BUCKET,
            path = %legacy_path,
            error = %err,
            "Skipping legacy tier config migration after read failure"
        ),
    }

    match read_tier_config_from_bucket(api.clone(), MIGRATING_META_BUCKET, &target_path, &opts).await {
        Ok(Some(data)) => match decode_tiering_config_blob(&data).and_then(|cfg| encode_external_tiering_config_blob(&cfg)) {
            Ok(out) => {
                if write_tier_config_to_rustfs(api.clone(), &target_path, out).await.is_ok() {
                    info!("Migrated compatible tier config from migrating metadata bucket");
                }
            }
            Err(err) => debug!(
                bucket = MIGRATING_META_BUCKET,
                path = %target_path,
                error = %err,
                "Skipping incompatible migrating tier config"
            ),
        },
        Ok(None) => {}
        Err(err) => debug!(
            bucket = MIGRATING_META_BUCKET,
            path = %target_path,
            error = %err,
            "Skipping migrating tier config after read failure"
        ),
    }
}

pub fn is_err_config_not_found(err: &StorageError) -> bool {
    matches!(err, StorageError::ObjectNotFound(_, _) | StorageError::BucketNotFound(_)) || err == &StorageError::ConfigNotFound
}

fn tier_config_not_initialized_error(operation: &str) -> std::io::Error {
    std::io::Error::other(format!("failed to {operation}: object layer not initialized"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::{
        endpoint::Endpoint,
        endpoints::{Endpoints, PoolEndpoints, SetupType},
    };
    use crate::services::tier::tier_mutation_intent::TIER_MUTATION_INTENT_RECORD_PREFIX;
    use s3s::dto::{
        BucketLifecycleConfiguration, ExpirationStatus, LifecycleRule, NoncurrentVersionTransition, Transition,
        TransitionStorageClass,
    };

    #[test]
    fn restore_retry_classifier_matches_only_the_exact_mutation_block() {
        assert!(TierConfigMgr::operation_lease_blocked_by_mutation(&AdminError::msg(
            TIER_MUTATION_BLOCK_MESSAGE,
        )));
        for message in [
            "Remote tier configuration is being replaced: COLD",
            "remote tier configuration is being replaced",
            "Remote tier configuration is unavailable",
            "",
        ] {
            assert!(
                !TierConfigMgr::operation_lease_blocked_by_mutation(&AdminError::msg(message)),
                "unrelated error must not consume the restore retry budget: {message}"
            );
        }
    }

    struct SetupTypeGuard {
        previous: SetupType,
    }

    impl SetupTypeGuard {
        async fn switch_to(next: SetupType) -> Self {
            let previous = runtime_sources::current_setup_type().await;
            runtime_sources::set_setup_type(next).await;
            Self { previous }
        }
    }

    impl Drop for SetupTypeGuard {
        fn drop(&mut self) {
            let previous = self.previous.clone();
            let handle = tokio::runtime::Handle::current();
            tokio::task::block_in_place(|| {
                handle.block_on(async move {
                    runtime_sources::set_setup_type(previous).await;
                });
            });
        }
    }

    fn build_s3_tier(name: &str) -> TierConfig {
        TierConfig {
            version: "v1".to_string(),
            tier_type: TierType::S3,
            name: name.to_string(),
            s3: Some(crate::services::tier::tier_config::TierS3 {
                name: name.to_string(),
                endpoint: "https://example-s3.invalid".to_string(),
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
                bucket: "bucket-a".to_string(),
                prefix: "prefix-a".to_string(),
                region: "us-east-1".to_string(),
                storage_class: "STANDARD".to_string(),
                aws_role: false,
                aws_role_web_identity_token_file: String::new(),
                aws_role_arn: String::new(),
                aws_role_session_name: String::new(),
                aws_role_duration_seconds: 0,
            }),
            ..Default::default()
        }
    }

    fn build_wasabi_tier(name: &str) -> TierConfig {
        TierConfig {
            version: "v1".to_string(),
            tier_type: TierType::Wasabi,
            name: name.to_string(),
            wasabi: Some(TierWasabi {
                name: name.to_string(),
                endpoint: "https://s3.ap-northeast-1.wasabisys.com".to_string(),
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
                bucket: "wasabi-bucket".to_string(),
                prefix: "archive".to_string(),
                region: "ap-northeast-1".to_string(),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn test_tiering_external_blob_roundtrip_for_standard_type() {
        let mut cfg = TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: HashMap::new(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        };
        cfg.tiers.insert("COLD-A".to_string(), build_s3_tier("COLD-A"));

        let bytes = encode_external_tiering_config_blob(&cfg).expect("encode should succeed");
        assert_eq!(&bytes[0..2], &TIER_CONFIG_FORMAT.to_le_bytes());
        assert_eq!(&bytes[2..4], &TIER_CONFIG_VERSION.to_le_bytes());

        let decoded = decode_external_tiering_config_blob(&bytes).expect("decode should succeed");
        let tier = decoded.tiers.get("COLD-A").expect("tier should exist");
        assert_eq!(tier.tier_type.as_lowercase(), "s3");
        assert_eq!(tier.s3.as_ref().expect("s3 should exist").endpoint, "https://example-s3.invalid");
    }

    #[test]
    fn test_tiering_external_blob_roundtrip_for_extended_type_hint() {
        let mut cfg = TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: HashMap::new(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        };
        cfg.tiers.insert(
            "COLD-B".to_string(),
            TierConfig {
                version: "v1".to_string(),
                tier_type: TierType::RustFS,
                name: "COLD-B".to_string(),
                rustfs: Some(crate::services::tier::tier_config::TierRustFS {
                    name: "COLD-B".to_string(),
                    endpoint: "https://example-compat.invalid".to_string(),
                    access_key: "ak".to_string(),
                    secret_key: "sk".to_string(),
                    bucket: "bucket-b".to_string(),
                    prefix: "prefix-b".to_string(),
                    region: "us-east-1".to_string(),
                    storage_class: "STANDARD".to_string(),
                }),
                ..Default::default()
            },
        );

        let bytes = encode_external_tiering_config_blob(&cfg).expect("encode should succeed");
        let decoded = decode_external_tiering_config_blob(&bytes).expect("decode should succeed");
        let tier = decoded.tiers.get("COLD-B").expect("tier should exist");
        assert_eq!(tier.tier_type.as_lowercase(), "rustfs");
        assert_eq!(
            tier.rustfs.as_ref().expect("backend should exist").endpoint,
            "https://example-compat.invalid"
        );
    }

    #[test]
    fn external_tier_config_encoding_and_digest_are_stable_for_multiple_tiers() {
        let mut first = empty_mgr();
        first.tiers.insert("COLD-Z".to_string(), build_rustfs_tier("COLD-Z"));
        first.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        let mut second = empty_mgr();
        second.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        second.tiers.insert("COLD-Z".to_string(), build_rustfs_tier("COLD-Z"));

        let expected = encode_external_tiering_config_blob(&first).expect("multi-tier config should encode");
        assert_eq!(
            encode_external_tiering_config_blob(&second).expect("reverse insertion order should encode"),
            expected,
            "tier config bytes must not depend on HashMap insertion or hash seed order"
        );
        let expected_digest = tier_config_candidate_digest(&first).expect("multi-tier digest should build");
        for _ in 0..16 {
            assert_eq!(
                encode_external_tiering_config_blob(&first).expect("repeated multi-tier config should encode"),
                expected
            );
            assert_eq!(
                tier_config_candidate_digest(&second).expect("repeated multi-tier digest should build"),
                expected_digest
            );
        }
    }

    #[tokio::test]
    async fn wasabi_external_blob_survives_old_node_rewrite_and_fences_old_drivers() {
        const WASABI_V2_GOLDEN: &[u8] = &[
            1, 0, 2, 0, 145, 129, 171, 67, 79, 76, 68, 45, 87, 65, 83, 65, 66, 73, 152, 176, 114, 117, 115, 116, 102, 115, 45,
            119, 97, 115, 97, 98, 105, 45, 118, 49, 1, 171, 67, 79, 76, 68, 45, 87, 65, 83, 65, 66, 73, 156, 177, 114, 117, 115,
            116, 102, 115, 45, 119, 97, 115, 97, 98, 105, 45, 118, 49, 58, 162, 97, 107, 162, 115, 107, 173, 119, 97, 115, 97,
            98, 105, 45, 98, 117, 99, 107, 101, 116, 167, 97, 114, 99, 104, 105, 118, 101, 174, 97, 112, 45, 110, 111, 114, 116,
            104, 101, 97, 115, 116, 45, 49, 160, 194, 160, 160, 160, 0, 192, 192, 192, 166, 119, 97, 115, 97, 98, 105,
        ];

        let mut cfg = empty_mgr();
        let mut tier = build_wasabi_tier("COLD-WASABI");
        tier.wasabi.as_mut().expect("Wasabi payload should exist").endpoint.clear();
        cfg.tiers.insert("COLD-WASABI".to_string(), tier);

        let bytes = encode_external_tiering_config_blob(&cfg).expect("Wasabi tier should encode");
        assert_eq!(bytes.as_ref(), WASABI_V2_GOLDEN, "Wasabi v2 bytes must remain stable");
        assert_eq!(&bytes[0..2], &TIER_CONFIG_FORMAT.to_le_bytes());
        assert_eq!(&bytes[2..4], &TIER_CONFIG_VERSION.to_le_bytes());
        let external: ExternalTierConfigMgr = rmp_serde::from_slice(&bytes[4..]).expect("external Wasabi payload should decode");
        let stored = external.tiers.get("COLD-WASABI").expect("stored Wasabi tier should exist");
        assert_eq!(stored.version, EXTERNAL_TIER_VERSION_WASABI_V1);
        assert_eq!(stored.tier_type, EXTERNAL_TIER_TYPE_S3);
        assert_eq!(stored.tier_type_hint.as_deref(), Some("wasabi"));
        assert!(stored.compatible_backend.is_none());
        let s3 = stored.s3.as_ref().expect("Wasabi should use the S3 payload");
        assert_eq!(s3.endpoint, EXTERNAL_TIER_WASABI_ENDPOINT_SENTINEL);
        assert_eq!(s3.bucket, "wasabi-bucket");
        assert_eq!(s3.prefix, "archive");
        assert_eq!(s3.region, "ap-northeast-1");
        assert!(s3.storage_class.is_empty());

        let decoded = decode_external_tiering_config_blob(&bytes).expect("Wasabi tier should decode");
        let wasabi = decoded.tiers["COLD-WASABI"]
            .wasabi
            .as_ref()
            .expect("Wasabi payload should survive roundtrip");
        assert_eq!(wasabi.endpoint, "https://s3.ap-northeast-1.wasabisys.com");
        assert_eq!(wasabi.secret_key, "sk");
        assert_eq!(decoded.tiers["COLD-WASABI"].version, EXTERNAL_TIER_VERSION_WASABI_V1);

        let encode_fixture = |external: &ExternalTierConfigMgr| {
            let payload = rmp_serde::to_vec(external).expect("Wasabi fixture should encode");
            let mut fixture = Vec::with_capacity(4 + payload.len());
            fixture.extend_from_slice(&TIER_CONFIG_FORMAT.to_le_bytes());
            fixture.extend_from_slice(&TIER_CONFIG_VERSION.to_le_bytes());
            fixture.extend_from_slice(&payload);
            fixture
        };

        // Current older nodes preserve the per-tier Version and S3 payload when
        // rewriting the binary document, but drop an unrecognized XTierType.
        let mut old_node_rewrite = external.clone();
        old_node_rewrite
            .tiers
            .get_mut("COLD-WASABI")
            .expect("stored Wasabi tier should exist")
            .tier_type_hint = None;
        let rewritten = encode_fixture(&old_node_rewrite);
        let decoded = decode_external_tiering_config_blob(&rewritten)
            .expect("the durable per-tier marker must restore Wasabi after an old-node rewrite");
        assert!(matches!(decoded.tiers["COLD-WASABI"].tier_type, TierType::Wasabi));

        let old_s3_payload = old_node_rewrite.tiers["COLD-WASABI"]
            .s3
            .as_ref()
            .expect("old nodes should retain the physical S3 payload");
        let old_s3 = crate::services::tier::tier_config::TierS3 {
            name: "COLD-WASABI".to_string(),
            endpoint: old_s3_payload.endpoint.clone(),
            access_key: old_s3_payload.access_key.clone(),
            secret_key: old_s3_payload.secret_key.clone(),
            bucket: old_s3_payload.bucket.clone(),
            prefix: old_s3_payload.prefix.clone(),
            region: old_s3_payload.region.clone(),
            ..Default::default()
        };
        let err = match crate::services::tier::warm_backend_s3::WarmBackendS3::new(&old_s3, "COLD-WASABI").await {
            Ok(_) => panic!("an older generic S3 driver must not operate a Wasabi compatibility envelope"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("missing a host"), "{err}");

        let mut generic_s3 = old_node_rewrite.clone();
        generic_s3
            .tiers
            .get_mut("COLD-WASABI")
            .expect("stored Wasabi tier should exist")
            .version = "v1".to_string();
        let decoded = decode_external_tiering_config_blob(&encode_fixture(&generic_s3))
            .expect("a generic S3 tier without an explicit marker must stay generic");
        assert!(matches!(decoded.tiers["COLD-WASABI"].tier_type, TierType::S3));

        let mut hint_only = external.clone();
        hint_only
            .tiers
            .get_mut("COLD-WASABI")
            .expect("stored Wasabi tier should exist")
            .version = "v1".to_string();
        let err = expect_decode_err(&encode_fixture(&hint_only));
        assert!(err.to_string().contains("inconsistent Wasabi type discriminators"), "{err}");

        let mut wrong_type = old_node_rewrite.clone();
        wrong_type
            .tiers
            .get_mut("COLD-WASABI")
            .expect("stored Wasabi tier should exist")
            .tier_type = EXTERNAL_TIER_TYPE_MINIO;
        let err = expect_decode_err(&encode_fixture(&wrong_type));
        assert!(err.to_string().contains("inconsistent Wasabi type discriminators"), "{err}");

        let mut wrong_hint = external.clone();
        wrong_hint
            .tiers
            .get_mut("COLD-WASABI")
            .expect("stored Wasabi tier should exist")
            .tier_type_hint = Some("rustfs".to_string());
        let err = expect_decode_err(&encode_fixture(&wrong_hint));
        assert!(err.to_string().contains("inconsistent Wasabi type discriminators"), "{err}");

        type WasabiPoisonField = (&'static str, fn(&mut ExternalTierS3));
        let poison_fields: [WasabiPoisonField; 6] = [
            ("storage_class", |s3| s3.storage_class = "GLACIER".to_string()),
            ("aws_role", |s3| s3.aws_role = true),
            ("web_identity_token", |s3| s3.aws_role_web_identity_token_file = "/tmp/token".to_string()),
            ("role_arn", |s3| s3.aws_role_arn = "arn:aws:iam::1:role/test".to_string()),
            ("role_session", |s3| s3.aws_role_session_name = "session".to_string()),
            ("role_duration", |s3| s3.aws_role_duration_seconds = 900),
        ];
        for (field, poison) in poison_fields {
            let mut unsupported_credentials = external.clone();
            let s3 = unsupported_credentials
                .tiers
                .get_mut("COLD-WASABI")
                .expect("stored Wasabi tier should exist")
                .s3
                .as_mut()
                .expect("Wasabi S3 payload should exist");
            poison(s3);
            let err = expect_decode_err(&encode_fixture(&unsupported_credentials));
            assert!(
                err.to_string().contains("unsupported Wasabi S3 credential fields"),
                "field {field}: {err}"
            );
        }
    }

    #[test]
    fn legacy_extended_type_hint_keeps_precedence_over_numeric_type() {
        let mut external = ExternalTierConfigMgr::default();
        external.tiers.insert(
            "COLD-LEGACY".to_string(),
            ExternalTierConfig {
                version: "v1".to_string(),
                tier_type: EXTERNAL_TIER_TYPE_MINIO,
                name: "COLD-LEGACY".to_string(),
                tier_type_hint: Some("rustfs".to_string()),
                s3: Some(ExternalTierS3 {
                    endpoint: "https://rustfs.example.invalid".to_string(),
                    access_key: "ak".to_string(),
                    secret_key: "sk".to_string(),
                    bucket: "archive".to_string(),
                    prefix: "objects".to_string(),
                    region: "us-east-1".to_string(),
                    ..Default::default()
                }),
                compatible_backend: Some(ExternalTierCompatible {
                    endpoint: "https://minio.example.invalid".to_string(),
                    bucket: "different".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        let payload = rmp_serde::to_vec(&external).expect("legacy fixture should encode");
        let mut blob = Vec::with_capacity(4 + payload.len());
        blob.extend_from_slice(&TIER_CONFIG_FORMAT.to_le_bytes());
        blob.extend_from_slice(&TIER_CONFIG_VERSION.to_le_bytes());
        blob.extend_from_slice(&payload);

        let decoded = decode_external_tiering_config_blob(&blob).expect("legacy extended hint should decode as before");
        let rustfs = decoded.tiers["COLD-LEGACY"]
            .rustfs
            .as_ref()
            .expect("recognized legacy hint must override the numeric type");
        assert_eq!(rustfs.endpoint, "https://rustfs.example.invalid");
        assert_eq!(rustfs.bucket, "archive");
    }

    #[test]
    fn wasabi_external_blob_rejects_noncanonical_endpoint_and_unsafe_region() {
        let mut cfg = empty_mgr();
        cfg.tiers.insert("COLD-WASABI".to_string(), build_wasabi_tier("COLD-WASABI"));
        let bytes = encode_external_tiering_config_blob(&cfg).expect("Wasabi tier should encode");
        let base: ExternalTierConfigMgr = rmp_serde::from_slice(&bytes[4..]).expect("external Wasabi payload should decode");

        for (endpoint, region, expected) in [
            ("https://s3.example.invalid", "ap-northeast-1", "invalid Wasabi compatibility endpoint"),
            (EXTERNAL_TIER_WASABI_ENDPOINT_SENTINEL, "ap.northeast-1", "invalid Wasabi region"),
        ] {
            let mut external = base.clone();
            let s3 = external
                .tiers
                .get_mut("COLD-WASABI")
                .and_then(|tier| tier.s3.as_mut())
                .expect("stored Wasabi S3 payload should exist");
            s3.endpoint = endpoint.to_string();
            s3.region = region.to_string();
            let payload = rmp_serde::to_vec(&external).expect("corrupt fixture should encode");
            let mut corrupt = Vec::with_capacity(4 + payload.len());
            corrupt.extend_from_slice(&TIER_CONFIG_FORMAT.to_le_bytes());
            corrupt.extend_from_slice(&TIER_CONFIG_VERSION.to_le_bytes());
            corrupt.extend_from_slice(&payload);

            let err = expect_decode_err(&corrupt);
            assert!(err.to_string().contains(expected), "{err}");
        }

        let mut external = base;
        let stored = external
            .tiers
            .get_mut("COLD-WASABI")
            .expect("stored Wasabi tier should exist");
        let s3 = stored.s3.take().expect("stored Wasabi S3 payload should exist");
        stored.compatible_backend = Some(external_tier_alias_from_compatible_payload(
            s3.endpoint,
            s3.access_key,
            s3.secret_key,
            s3.bucket,
            s3.prefix,
            s3.region,
        ));
        let payload = rmp_serde::to_vec(&external).expect("wrong-payload fixture should encode");
        let mut corrupt = Vec::with_capacity(4 + payload.len());
        corrupt.extend_from_slice(&TIER_CONFIG_FORMAT.to_le_bytes());
        corrupt.extend_from_slice(&TIER_CONFIG_VERSION.to_le_bytes());
        corrupt.extend_from_slice(&payload);
        let err = expect_decode_err(&corrupt);
        assert!(err.to_string().contains("missing Wasabi S3 payload"), "{err}");
    }

    #[test]
    fn test_decode_tiering_config_blob_accepts_legacy_json() {
        let mut cfg = TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: HashMap::new(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        };
        cfg.tiers.insert("COLD-A".to_string(), build_s3_tier("COLD-A"));

        let data = serde_json::to_vec(&cfg).expect("legacy json should encode");
        let decoded = decode_tiering_config_blob(&data).expect("legacy json should decode");
        assert_eq!(
            decoded
                .tiers
                .get("COLD-A")
                .and_then(|tier| tier.s3.as_ref())
                .map(|s3| s3.bucket.as_str()),
            Some("bucket-a")
        );
    }

    #[test]
    fn wasabi_legacy_json_rejects_missing_payload_and_invalid_endpoints() {
        let mut cfg = empty_mgr();
        let mut tier = build_wasabi_tier("COLD-WASABI");
        tier.wasabi.as_mut().expect("Wasabi payload should exist").endpoint = "https://s3.example.invalid".to_string();
        cfg.tiers.insert("COLD-WASABI".to_string(), tier);

        let data = serde_json::to_vec(&cfg).expect("legacy JSON fixture should encode");
        let err = match TierConfigMgr::unmarshal(&data) {
            Ok(_) => panic!("legacy Wasabi endpoint mismatch must fail closed"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("https://s3.ap-northeast-1.wasabisys.com"));

        cfg.tiers
            .get_mut("COLD-WASABI")
            .and_then(|tier| tier.wasabi.as_mut())
            .expect("Wasabi payload should exist")
            .endpoint
            .clear();
        let data = serde_json::to_vec(&cfg).expect("empty-endpoint fixture should encode");
        let err = match TierConfigMgr::unmarshal(&data) {
            Ok(_) => panic!("legacy Wasabi endpoint must not be empty"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("missing Wasabi endpoint"), "{err}");

        cfg.tiers.get_mut("COLD-WASABI").expect("Wasabi tier should exist").wasabi = None;
        let data = serde_json::to_vec(&cfg).expect("missing-payload fixture should encode");
        let err = match TierConfigMgr::unmarshal(&data) {
            Ok(_) => panic!("legacy Wasabi payload must exist"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("missing Wasabi backend payload"), "{err}");
    }

    #[test]
    fn test_tier_config_not_initialized_error_formats_operation_context() {
        let err = tier_config_not_initialized_error("save tiering config");
        let rendered = err.to_string();

        assert!(rendered.contains("failed to save tiering config"), "{rendered}");
        assert!(rendered.contains("object layer not initialized"), "{rendered}");
    }

    // ---------------------------------------------------------------------
    // State-machine coverage (ilm-4): add / edit / remove / verify.
    //
    // These tests must not reach a real remote tier. Two techniques keep them
    // hermetic:
    //   * error paths that return *before* `build_warm_backend` constructs a
    //     client (name validation, duplicate detection, unsupported type,
    //     missing backend payload, missing credentials);
    //   * a `MockWarmBackend` injected directly into `driver_cache`, so
    //     `get_driver` returns it from cache without any network I/O, which
    //     lets us drive `remove`/`verify` through every branch.
    // ---------------------------------------------------------------------

    use crate::services::tier::test_util::{MockWarmBackend as RecordingWarmBackend, MockWarmOp};
    use crate::services::tier::warm_backend::{WarmBackend, WarmBackendGetOpts};
    use rustfs_s3_client::transition_api::{ReadCloser, ReaderImpl};

    fn empty_mgr() -> TierConfigMgr {
        TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: HashMap::new(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        }
    }

    fn build_rustfs_tier(name: &str) -> TierConfig {
        TierConfig {
            version: "v1".to_string(),
            tier_type: TierType::RustFS,
            name: name.to_string(),
            rustfs: Some(crate::services::tier::tier_config::TierRustFS {
                name: name.to_string(),
                endpoint: "https://example-compat.invalid".to_string(),
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
                bucket: "bucket-r".to_string(),
                prefix: "prefix-r".to_string(),
                region: "us-east-1".to_string(),
                storage_class: "STANDARD".to_string(),
            }),
            ..Default::default()
        }
    }

    fn build_gcs_tier(name: &str, credentials: &str) -> TierConfig {
        TierConfig {
            version: "v1".to_string(),
            tier_type: TierType::GCS,
            name: name.to_string(),
            gcs: Some(crate::services::tier::tier_config::TierGCS {
                name: name.to_string(),
                endpoint: "https://storage.googleapis.com".to_string(),
                creds: credentials.to_string(),
                bucket: "bucket-gcs".to_string(),
                prefix: "prefix-gcs".to_string(),
                region: String::new(),
                storage_class: String::new(),
            }),
            ..Default::default()
        }
    }

    #[derive(Debug)]
    struct LockingTierConfigStore {
        locks: Mutex<Vec<(String, String)>>,
        put_after_lock: AtomicBool,
        lock_manager: Arc<rustfs_lock::GlobalLockManager>,
    }

    impl LockingTierConfigStore {
        fn new() -> Self {
            Self {
                locks: Mutex::new(Vec::new()),
                put_after_lock: AtomicBool::new(false),
                lock_manager: Arc::new(rustfs_lock::GlobalLockManager::new()),
            }
        }

        fn lock_events(&self) -> Vec<(String, String)> {
            self.locks
                .lock()
                .expect("tier config lock recorder should not poison")
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl ObjectIO for LockingTierConfigStore {
        type Error = Error;
        type RangeSpec = HTTPRangeSpec;
        type HeaderMap = HeaderMap;
        type ObjectOptions = ObjectOptions;
        type ObjectInfo = ObjectInfo;
        type GetObjectReader = GetObjectReader;
        type PutObjectReader = PutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<HTTPRangeSpec>,
            _h: HeaderMap,
            _opts: &ObjectOptions,
        ) -> Result<GetObjectReader> {
            Err(Error::ObjectNotFound(bucket.to_string(), object.to_string()))
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            _data: &mut PutObjReader,
            opts: &ObjectOptions,
        ) -> Result<ObjectInfo> {
            let expected_lock = (RUSTFS_META_BUCKET.to_string(), tier_config_lock_path());
            let saw_expected_lock = self
                .locks
                .lock()
                .expect("tier config lock recorder should not poison")
                .contains(&expected_lock);
            self.put_after_lock.store(saw_expected_lock, Ordering::SeqCst);
            assert!(opts.max_parity, "tier config save must retain max parity");
            assert!(
                opts.http_preconditions
                    .as_ref()
                    .and_then(|preconditions| preconditions.if_none_match.as_deref())
                    == Some("*"),
                "initial tier config save must retain if-none-match protection"
            );
            Ok(ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                etag: Some("saved-etag".to_string()),
                ..Default::default()
            })
        }
    }

    #[async_trait::async_trait]
    impl ObjectOperations for LockingTierConfigStore {
        type Error = Error;
        type ObjectInfo = ObjectInfo;
        type ObjectOptions = ObjectOptions;
        type FileInfo = FileInfo;
        type ObjectToDelete = ObjectToDelete;
        type DeletedObject = DeletedObject;

        async fn get_object_info(&self, bucket: &str, object: &str, _opts: &Self::ObjectOptions) -> Result<Self::ObjectInfo> {
            Err(Error::ObjectNotFound(bucket.to_string(), object.to_string()))
        }

        async fn verify_object_integrity(&self, _bucket: &str, _object: &str, _opts: &Self::ObjectOptions) -> Result<()> {
            Err(Error::NotImplemented)
        }

        async fn copy_object(
            &self,
            _src_bucket: &str,
            _src_object: &str,
            _dst_bucket: &str,
            _dst_object: &str,
            _src_info: &mut Self::ObjectInfo,
            _src_opts: &Self::ObjectOptions,
            _dst_opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo> {
            Err(Error::NotImplemented)
        }

        async fn delete_object_version(
            &self,
            _bucket: &str,
            _object: &str,
            _fi: &Self::FileInfo,
            _force_del_marker: bool,
        ) -> Result<()> {
            Err(Error::NotImplemented)
        }

        async fn delete_object(&self, bucket: &str, object: &str, _opts: Self::ObjectOptions) -> Result<Self::ObjectInfo> {
            Err(Error::ObjectNotFound(bucket.to_string(), object.to_string()))
        }

        async fn delete_objects(
            &self,
            _bucket: &str,
            _objects: Vec<Self::ObjectToDelete>,
            _opts: Self::ObjectOptions,
        ) -> (Vec<Self::DeletedObject>, Vec<Option<Self::Error>>) {
            (Vec::new(), vec![Some(Error::NotImplemented)])
        }

        async fn put_object_metadata(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo> {
            Err(Error::NotImplemented)
        }

        async fn get_object_tags(&self, _bucket: &str, _object: &str, _opts: &Self::ObjectOptions) -> Result<String> {
            Err(Error::NotImplemented)
        }

        async fn put_object_tags(
            &self,
            _bucket: &str,
            _object: &str,
            _tags: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo> {
            Err(Error::NotImplemented)
        }

        async fn delete_object_tags(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo> {
            Err(Error::NotImplemented)
        }

        async fn add_partial(&self, _bucket: &str, _object: &str, _version_id: &str) -> Result<()> {
            Err(Error::NotImplemented)
        }

        async fn transition_object(&self, _bucket: &str, _object: &str, _opts: &Self::ObjectOptions) -> Result<()> {
            Err(Error::NotImplemented)
        }

        async fn restore_transitioned_object(
            self: Arc<Self>,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<()> {
            Err(Error::NotImplemented)
        }
    }

    #[async_trait::async_trait]
    impl BucketOperations for LockingTierConfigStore {
        type Error = Error;

        async fn make_bucket(
            &self,
            _bucket: &str,
            _opts: &crate::storage_api_contracts::bucket::MakeBucketOptions,
        ) -> Result<()> {
            Err(Error::NotImplemented)
        }

        async fn get_bucket_info(
            &self,
            _bucket: &str,
            _opts: &crate::storage_api_contracts::bucket::BucketOptions,
        ) -> Result<crate::storage_api_contracts::bucket::BucketInfo> {
            Err(Error::NotImplemented)
        }

        async fn list_bucket(
            &self,
            _opts: &crate::storage_api_contracts::bucket::BucketOptions,
        ) -> Result<Vec<crate::storage_api_contracts::bucket::BucketInfo>> {
            Ok(Vec::new())
        }

        async fn delete_bucket(
            &self,
            _bucket: &str,
            _opts: &crate::storage_api_contracts::bucket::DeleteBucketOptions,
        ) -> Result<()> {
            Err(Error::NotImplemented)
        }
    }

    #[async_trait::async_trait]
    impl ListOperations for LockingTierConfigStore {
        type Error = Error;
        type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
        type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
        type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
        type WalkOptions = TierReferenceProofWalkOptions;
        type WalkCancellation = tokio_util::sync::CancellationToken;
        type WalkResultSender = tokio::sync::mpsc::Sender<StorageObjectInfoOrErr<ObjectInfo, Error>>;

        async fn list_objects_v2(
            self: Arc<Self>,
            _bucket: &str,
            _prefix: &str,
            _continuation_token: Option<String>,
            _delimiter: Option<String>,
            _max_keys: i32,
            _fetch_owner: bool,
            _start_after: Option<String>,
            _incl_deleted: bool,
        ) -> Result<Self::ListObjectsV2Info> {
            Ok(StorageListObjectsV2Info::default())
        }

        async fn list_object_versions(
            self: Arc<Self>,
            _bucket: &str,
            _prefix: &str,
            _marker: Option<String>,
            _version_marker: Option<String>,
            _delimiter: Option<String>,
            _max_keys: i32,
        ) -> Result<Self::ListObjectVersionsInfo> {
            Ok(StorageListObjectVersionsInfo::default())
        }

        async fn walk(
            self: Arc<Self>,
            _rx: Self::WalkCancellation,
            _bucket: &str,
            _prefix: &str,
            _result: Self::WalkResultSender,
            _opts: Self::WalkOptions,
        ) -> Result<()> {
            Err(Error::NotImplemented)
        }
    }

    #[async_trait::async_trait]
    impl NamespaceLocking for LockingTierConfigStore {
        type Error = Error;
        type NamespaceLock = rustfs_lock::NamespaceLockWrapper;

        async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<rustfs_lock::NamespaceLockWrapper> {
            self.locks
                .lock()
                .expect("tier config lock recorder should not poison")
                .push((bucket.to_string(), object.to_string()));
            Ok(rustfs_lock::NamespaceLockWrapper::new(
                rustfs_lock::NamespaceLock::with_local_manager("tier-config-test".to_string(), self.lock_manager.clone()),
                rustfs_lock::ObjectKey {
                    bucket: Arc::from(bucket),
                    object: Arc::from(object),
                    version: None,
                },
                "tier-config-test-owner".to_string(),
            ))
        }
    }

    #[async_trait::async_trait]
    impl TierReferenceProofStore for LockingTierConfigStore {
        async fn lifecycle_config_for_reference_proof(&self, _bucket: &str) -> Result<Option<BucketLifecycleConfiguration>> {
            Ok(None)
        }
    }

    #[tokio::test]
    async fn tier_config_update_path_acquires_meta_namespace_sidecar_lock_before_save() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(LockingTierConfigStore::new());

        TierConfigMgr::update_candidate_with_config_lock(&manager, store.clone(), TierCandidateMutation::Clear(true))
            .await
            .expect("empty clear should still acquire and save through the coordinator lock");

        let expected_lock = (RUSTFS_META_BUCKET.to_string(), tier_config_lock_path());
        assert_eq!(
            store.lock_events(),
            vec![expected_lock.clone(), expected_lock.clone(), expected_lock],
            "the update should lock for the validation snapshot, durable Prepare, and config commit"
        );
        assert!(
            store.put_after_lock.load(Ordering::SeqCst),
            "tier config save must happen after the coordinator namespace lock is acquired"
        );
    }

    #[test]
    fn tier_mutation_targets_prove_remove_and_rebind_authoritative_identity() {
        let mut current = empty_mgr();
        current.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));

        let mut rebound = empty_mgr();
        let mut replacement = build_rustfs_tier("COLD-A");
        replacement.rustfs.as_mut().expect("replacement payload should exist").prefix = "new-prefix".to_string();
        rebound.tiers.insert("COLD-A".to_string(), replacement);

        let edit_targets = TierCandidateMutation::Edit("COLD-A".to_string(), TierCreds::default())
            .affected_targets(&current, &rebound)
            .expect("rebind target proof should build");
        assert_eq!(edit_targets.len(), 1);
        assert_eq!(edit_targets[0].tier_name, "COLD-A");
        assert!(edit_targets[0].old_backend_identity.is_some());
        assert!(edit_targets[0].new_backend_identity.is_some());
        assert_ne!(edit_targets[0].old_backend_identity, edit_targets[0].new_backend_identity);

        let removed = empty_mgr();
        let remove_targets = TierCandidateMutation::Remove("COLD-A".to_string(), true)
            .affected_targets(&current, &removed)
            .expect("remove target proof should build");
        assert_eq!(remove_targets.len(), 1);
        assert_eq!(remove_targets[0].tier_name, "COLD-A");
        assert!(remove_targets[0].old_backend_identity.is_some());
        assert!(remove_targets[0].new_backend_identity.is_none());

        let noop_targets = TierCandidateMutation::Remove("MISSING".to_string(), true)
            .affected_targets(&current, &current)
            .expect("idempotent remove of a missing tier should not require an identity proof");
        assert!(noop_targets.is_empty());
    }

    /// A fully offline `WarmBackend` used to exercise the driver-facing
    /// branches of `remove`/`verify` without touching a remote tier.
    struct MockWarmBackend {
        /// `Some(b)` -> `in_use()` returns `Ok(b)`; `None` -> returns `Err`.
        in_use_value: Option<bool>,
        /// When false, put/get/remove all fail (drives `verify` error paths).
        healthy: bool,
        probe_present: AtomicBool,
    }

    #[derive(Default)]
    struct HangingInUseBackend {
        probe_present: AtomicBool,
    }

    #[async_trait::async_trait]
    impl WarmBackend for HangingInUseBackend {
        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> io::Result<String> {
            self.probe_present.store(true, Ordering::SeqCst);
            Ok("probe-version".to_string())
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> io::Result<String> {
            self.put(object, r, length).await
        }

        async fn get(&self, _object: &str, _rv: &str, _opts: WarmBackendGetOpts) -> io::Result<ReadCloser> {
            Ok(BufReader::new(Cursor::new(b"RustFS".to_vec())))
        }

        async fn remove(&self, _object: &str, _rv: &str) -> io::Result<()> {
            self.probe_present.store(false, Ordering::SeqCst);
            Ok(())
        }

        async fn probe_transition_candidate(&self, _object: &str) -> io::Result<TransitionCandidateProbe> {
            if self.probe_present.load(Ordering::SeqCst) {
                Ok(TransitionCandidateProbe::VersionedPresent("probe-version".to_string()))
            } else {
                Ok(TransitionCandidateProbe::Missing)
            }
        }

        async fn in_use(&self) -> io::Result<bool> {
            std::future::pending().await
        }
    }

    #[async_trait::async_trait]
    impl WarmBackend for MockWarmBackend {
        async fn validate(&self) -> std::result::Result<(), std::io::Error> {
            if self.healthy {
                Ok(())
            } else {
                Err(std::io::Error::other("mock validation failed"))
            }
        }

        fn validate_remote_version_id(&self, remote_version_id: &str) -> std::result::Result<(), std::io::Error> {
            if remote_version_id == "unsupported-version" {
                Err(std::io::Error::other("mock remote version rejected"))
            } else {
                Ok(())
            }
        }

        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> std::result::Result<String, std::io::Error> {
            if self.healthy {
                self.probe_present.store(true, Ordering::SeqCst);
                Ok("mock-version".to_string())
            } else {
                Err(std::io::Error::other("mock put failed"))
            }
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> std::result::Result<String, std::io::Error> {
            self.put(object, r, length).await
        }

        async fn get(
            &self,
            _object: &str,
            _rv: &str,
            _opts: WarmBackendGetOpts,
        ) -> std::result::Result<ReadCloser, std::io::Error> {
            if self.healthy {
                Ok(BufReader::new(Cursor::new(b"RustFS".to_vec())))
            } else {
                Err(std::io::Error::other("mock get failed"))
            }
        }

        async fn remove(&self, _object: &str, _rv: &str) -> std::result::Result<(), std::io::Error> {
            if self.healthy {
                self.probe_present.store(false, Ordering::SeqCst);
                Ok(())
            } else {
                Err(std::io::Error::other("mock remove failed"))
            }
        }

        async fn remove_exact(&self, object: &str, rv: &str) -> std::result::Result<(), std::io::Error> {
            if rv == "exact-only" {
                return Err(std::io::Error::other("mock exact remove forwarded"));
            }
            self.remove(object, rv).await
        }

        async fn probe_transition_candidate(
            &self,
            _object: &str,
        ) -> std::result::Result<TransitionCandidateProbe, std::io::Error> {
            if !self.healthy {
                return Err(std::io::Error::other("mock candidate probe failed"));
            }
            if self.probe_present.load(Ordering::SeqCst) {
                Ok(TransitionCandidateProbe::VersionedPresent("mock-version".to_string()))
            } else {
                Ok(TransitionCandidateProbe::Missing)
            }
        }

        async fn in_use(&self) -> std::result::Result<bool, std::io::Error> {
            match self.in_use_value {
                Some(b) => Ok(b),
                None => Err(std::io::Error::other("mock in_use failed")),
            }
        }
    }

    fn inject_mock(mgr: &mut TierConfigMgr, name: &str, tier: TierConfig, mock: MockWarmBackend) {
        mgr.tiers.insert(name.to_string(), tier);
        mgr.driver_cache.insert(name.to_string(), Box::new(mock));
    }

    fn recording_driver_factory(
        backend: RecordingWarmBackend,
        observed_configs: Arc<Mutex<Vec<TierConfig>>>,
    ) -> TierDriverTestFactory {
        Arc::new(move |config| {
            lock_unpoisoned(&observed_configs).push(config.clone_with_credentials());
            Ok(Box::new(backend.clone()))
        })
    }

    fn healthy_driver_factory() -> TierDriverTestFactory {
        recording_driver_factory(RecordingWarmBackend::new(), Arc::new(Mutex::new(Vec::new())))
    }

    // ---- add ------------------------------------------------------------

    #[test]
    fn test_add_name_normalization_uses_canonical_name_and_supports_legacy_nested_name() {
        let mut canonical_s3 = build_s3_tier("COLD-CANONICAL");
        canonical_s3.s3.as_mut().expect("S3 payload should exist").name.clear();
        normalize_s3_gcs_add_tier_name(&mut canonical_s3).expect("canonical S3 name should normalize");
        assert_eq!(canonical_s3.name, "COLD-CANONICAL");
        assert_eq!(canonical_s3.s3.as_ref().expect("S3 payload should remain").name, "COLD-CANONICAL");

        let mut legacy_gcs = build_gcs_tier("COLD-LEGACY", r#"{"type":"service_account"}"#);
        legacy_gcs.name.clear();
        normalize_s3_gcs_add_tier_name(&mut legacy_gcs).expect("legacy nested GCS name should normalize");
        assert_eq!(legacy_gcs.name, "COLD-LEGACY");
        assert_eq!(legacy_gcs.gcs.as_ref().expect("GCS payload should remain").name, "COLD-LEGACY");
    }

    #[tokio::test]
    async fn test_add_rejects_non_uppercase_name() {
        let mut mgr = empty_mgr();
        let err = mgr
            .add(build_s3_tier("cold-a"), true)
            .await
            .expect_err("lowercase tier name must be rejected");
        assert_eq!(err.code, ERR_TIER_NAME_NOT_UPPERCASE.code);
        assert!(mgr.tiers.is_empty(), "rejected tier must not be persisted");
    }

    #[tokio::test]
    async fn test_add_rejects_duplicate_name() {
        let mut mgr = empty_mgr();
        mgr.tiers.insert("COLD-A".to_string(), build_s3_tier("COLD-A"));

        let err = mgr
            .add(build_s3_tier("COLD-A"), true)
            .await
            .expect_err("duplicate tier name must be rejected");
        assert_eq!(err.code, ERR_TIER_ALREADY_EXISTS.code);
    }

    #[tokio::test]
    async fn test_add_rejects_unsupported_tier_type() {
        let mut mgr = empty_mgr();
        // `Unsupported` is rejected by `new_warm_backend` before any network I/O.
        let tier = TierConfig {
            version: "v1".to_string(),
            tier_type: TierType::Unsupported,
            name: "COLD-U".to_string(),
            ..Default::default()
        };
        let err = mgr.add(tier, true).await.expect_err("unsupported tier type must be rejected");
        assert_eq!(err.code, ERR_TIER_TYPE_UNSUPPORTED.code);
        assert!(mgr.tiers.is_empty());
    }

    #[tokio::test]
    async fn test_add_rejects_missing_backend_payload() {
        let mut mgr = empty_mgr();
        // Declares S3 type but omits the S3 payload; rejected before client build.
        let tier = TierConfig {
            version: "v1".to_string(),
            tier_type: TierType::S3,
            name: "COLD-A".to_string(),
            s3: None,
            ..Default::default()
        };
        let err = mgr
            .add(tier, true)
            .await
            .expect_err("missing backend payload must be rejected");
        assert_eq!(err.code, "XRustFSAdminTierInvalidConfig");
        assert!(err.message.contains("S3 tier configuration not found"), "{}", err.message);
    }

    #[tokio::test]
    async fn test_add_rejects_custom_wasabi_endpoint_before_backend_setup() {
        let mut mgr = empty_mgr();
        let mut tier = build_wasabi_tier("COLD-WASABI");
        tier.wasabi.as_mut().expect("Wasabi payload should exist").endpoint = "https://s3.example.invalid".to_string();

        let err = mgr
            .add(tier, true)
            .await
            .expect_err("custom Wasabi endpoint must fail before backend setup");
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert_eq!(err.message, "Wasabi endpoint must be https://s3.ap-northeast-1.wasabisys.com");
        assert!(mgr.tiers.is_empty());
    }

    #[tokio::test]
    async fn test_add_rejects_azure_storage_class_before_backend_setup() {
        let mut mgr = empty_mgr();
        let mut tier = build_azure_tier("account-a");
        tier.azure.as_mut().expect("Azure payload should exist").storage_class = "HOT".to_string();

        let err = mgr
            .add(tier, true)
            .await
            .expect_err("a non-empty Azure storageClass must be rejected before backend setup");
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert!(err.message.contains("storageClass"), "{}", err.message);
        assert!(mgr.tiers.is_empty());
    }

    #[tokio::test]
    async fn test_add_rejects_azure_partial_sp_auth_before_backend_setup() {
        // Only `tenant_id` is set: `TierAzure::is_sp_enabled()`-style "all three fields"
        // logic would miss this, so the check must reject on *any* sp_auth sub-field
        // being non-empty rather than requiring all three.
        let mut mgr = empty_mgr();
        let mut tier = build_azure_tier("account-a");
        tier.azure.as_mut().expect("Azure payload should exist").sp_auth.tenant_id = "tenant".to_string();

        let err = mgr
            .add(tier, true)
            .await
            .expect_err("a partially-filled Azure spAuth must be rejected before backend setup");
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert!(err.message.contains("spAuth"), "{}", err.message);
        assert!(mgr.tiers.is_empty());
    }

    #[tokio::test]
    async fn test_add_rejects_canonical_azure_sp_auth_wire_before_backend_setup() {
        let tier: TierConfig = serde_json::from_value(serde_json::json!({
            "type": "azure",
            "Name": "COLD-AZURE",
            "azure": {
                "name": "COLD-AZURE",
                "endpoint": "https://azure.example.invalid",
                "accessKey": "account",
                "secretKey": "key",
                "bucket": "archive",
                "spAuth": {
                    "TenantID": "tenant"
                }
            }
        }))
        .expect("mixed RustFS/madmin Azure payload should decode");
        let backend_builds = Arc::new(AtomicUsize::new(0));
        let observed_builds = backend_builds.clone();
        let factory: TierDriverTestFactory = Arc::new(move |_| {
            observed_builds.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(RecordingWarmBackend::new()))
        });
        let mut mgr = empty_mgr();

        let err = TIER_DRIVER_TEST_FACTORY
            .scope(factory, mgr.add(tier, true))
            .await
            .expect_err("canonical Azure service-principal fields must fail closed");

        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert!(err.message.contains("spAuth"), "{}", err.message);
        assert_eq!(backend_builds.load(Ordering::SeqCst), 0);
        assert!(mgr.tiers.is_empty());
    }

    #[tokio::test]
    async fn test_add_does_not_reject_azure_config_without_storage_class_or_sp_auth() {
        // A plain Azure config (the common case: static access/secret key, no
        // storageClass, no spAuth) must sail past the provider-specific gate.
        let mut mgr = empty_mgr();
        let tier = build_azure_tier("account-a");
        let tier_name = tier.name.clone();

        TIER_DRIVER_TEST_FACTORY
            .scope(healthy_driver_factory(), mgr.add(tier, true))
            .await
            .expect("a config with no storageClass/spAuth must not trip the new gate");
        assert!(mgr.tiers.contains_key(&tier_name));
    }

    #[tokio::test]
    async fn test_add_force_does_not_bypass_bad_credentials_or_unreachable_probe() {
        for unreachable in [false, true] {
            let mut mgr = empty_mgr();
            let backend = RecordingWarmBackend::new();
            if unreachable {
                backend.set_unreachable(true).await;
            } else {
                backend.set_reject_credentials(true).await;
            }
            let observed = Arc::new(Mutex::new(Vec::new()));

            let err = TIER_DRIVER_TEST_FACTORY
                .scope(
                    recording_driver_factory(backend, observed.clone()),
                    mgr.add(build_s3_tier("COLD-A"), true),
                )
                .await
                .expect_err("force must not bypass the backend read/write/delete probe");

            assert_eq!(err.code, ERR_TIER_PERM_ERR.code);
            assert_eq!(lock_unpoisoned(&observed).len(), 1);
            assert!(mgr.tiers.is_empty());
        }
    }

    #[tokio::test]
    async fn test_add_rejects_unsupported_s3_role_before_backend_setup() {
        let mut mgr = empty_mgr();
        let mut tier = build_s3_tier("COLD-A");
        let s3 = tier.s3.as_mut().expect("S3 payload should exist");
        s3.access_key.clear();
        s3.secret_key.clear();
        s3.aws_role = true;

        let err = mgr
            .add(tier, true)
            .await
            .expect_err("unsupported role credentials must fail before backend setup");

        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert!(err.message.contains("not supported"));
        assert!(mgr.tiers.is_empty());
    }

    #[tokio::test]
    async fn test_add_rejects_reserved_names() {
        // Supersedes the former `test_add_does_not_reserve_standard_name_regression_anchor`
        // (added by PR #4713 to pin the pre-fix behavior). RustFS now rejects the
        // AWS/MinIO reserved storage-class names STANDARD and REDUCED_REDUNDANCY
        // (RRS) as remote-tier names, matching MinIO parity.
        //
        // `tier_type` is deliberately `Unsupported`: before the guard existed,
        // "STANDARD" reached `new_warm_backend` and failed with
        // `ERR_TIER_TYPE_UNSUPPORTED`. Asserting `ERR_TIER_RESERVED_NAME` here
        // proves the reserved-name guard fires *before* the type/backend check.
        for reserved in [crate::config::storageclass::STANDARD, crate::config::storageclass::RRS] {
            let mut mgr = empty_mgr();
            let tier = TierConfig {
                version: "v1".to_string(),
                tier_type: TierType::Unsupported,
                name: reserved.to_string(),
                ..Default::default()
            };
            let err = mgr.add(tier, true).await.expect_err("reserved tier name must be rejected");
            assert_eq!(
                err.code, ERR_TIER_RESERVED_NAME.code,
                "reserved tier name {reserved} must be rejected before the type check"
            );
            assert!(mgr.tiers.is_empty(), "rejected reserved tier {reserved} must not be persisted");
        }
    }

    #[tokio::test]
    async fn test_add_rejects_canonical_s3_role_fields_even_with_static_credentials() {
        let mut tier: TierConfig = serde_json::from_value(serde_json::json!({
            "Version": "v1",
            "Type": "s3",
            "Name": "COLD-A",
            "S3": {
                "Endpoint": "https://s3.example.invalid",
                "AccessKey": "static-access",
                "SecretKey": "static-secret",
                "Bucket": "archive",
                "Prefix": "objects",
                "Region": "us-east-1",
                "StorageClass": "STANDARD",
                "AWSRole": true,
                "AWSRoleWebIdentityTokenFile": "/var/run/private-token",
                "AWSRoleARN": "arn:aws:iam::123456789012:role/archive",
                "AWSRoleSessionName": "archive-session",
                "AWSRoleDurationSeconds": 900
            }
        }))
        .expect("canonical madmin S3 role payload should decode");
        assert_eq!(tier.name, "COLD-A");
        let s3 = tier.s3.as_ref().expect("S3 payload should decode");
        assert!(s3.name.is_empty(), "canonical madmin S3 has no nested Name field");
        assert_eq!(s3.endpoint, "https://s3.example.invalid");
        assert!(!s3.access_key.is_empty());
        assert!(!s3.secret_key.is_empty());
        assert_eq!(s3.bucket, "archive");
        assert_eq!(s3.prefix, "objects");
        assert_eq!(s3.region, "us-east-1");
        assert_eq!(s3.storage_class, "STANDARD");
        assert!(s3.aws_role);
        assert_eq!(s3.aws_role_web_identity_token_file, "/var/run/private-token");
        assert_eq!(s3.aws_role_duration_seconds, 900);
        normalize_s3_gcs_add_tier_name(&mut tier).expect("canonical madmin Name should normalize before validation");
        assert_eq!(tier.name, "COLD-A");
        assert_eq!(tier.s3.as_ref().expect("S3 payload should remain").name, "COLD-A");
        let serialized = serde_json::to_value(&tier).expect("S3 config should serialize");
        assert_eq!(serialized["type"], "s3");
        assert!(serialized.get("Type").is_none());
        assert!(serialized.get("S3").is_none());
        assert!(serialized.get("Name").is_none());
        assert!(serialized["s3"].get("AccessKey").is_none());
        assert!(serialized["s3"].get("accessKey").is_some());
        assert!(serialized["s3"].get("AWSRole").is_none());
        assert!(serialized["s3"].get("AWSRoleWebIdentityTokenFile").is_none());

        let mut mgr = empty_mgr();
        let backend_builds = Arc::new(AtomicUsize::new(0));
        let observed_backend_builds = backend_builds.clone();
        let factory: TierDriverTestFactory = Arc::new(move |_| {
            observed_backend_builds.fetch_add(1, Ordering::SeqCst);
            Ok(Box::new(RecordingWarmBackend::new()))
        });
        let err = TIER_DRIVER_TEST_FACTORY
            .scope(factory, mgr.add(tier, true))
            .await
            .expect_err("unsupported role fields must be rejected before backend setup");
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert!(err.message.contains("not supported"));
        assert_eq!(backend_builds.load(Ordering::SeqCst), 0);
        assert!(mgr.tiers.is_empty());
    }

    #[tokio::test]
    async fn test_add_rejects_conflicting_canonical_and_legacy_s3_gcs_names_before_backend_setup() {
        let fixtures = [
            (
                "S3",
                serde_json::json!({
                    "Type": "S3",
                    "Name": "COLD-CANONICAL",
                    "S3": {
                        "name": "COLD-LEGACY",
                        "Endpoint": "https://s3.example.invalid",
                        "AccessKey": "access",
                        "SecretKey": "secret",
                        "Bucket": "archive"
                    }
                }),
            ),
            (
                "GCS",
                serde_json::json!({
                    "Type": "GCS",
                    "Name": "COLD-CANONICAL",
                    "GCS": {
                        "name": "COLD-LEGACY",
                        "Endpoint": "https://storage.googleapis.com/",
                        "Creds": "e30=",
                        "Bucket": "archive"
                    }
                }),
            ),
        ];

        for (provider, fixture) in fixtures {
            let tier: TierConfig = serde_json::from_value(fixture)
                .unwrap_or_else(|err| panic!("{provider} aliases should decode before name validation: {err}"));
            let manager = TierConfigMgr::new();
            let store = Arc::new(CasConfigStore::default());
            let backend_builds = Arc::new(AtomicUsize::new(0));
            let observed_backend_builds = backend_builds.clone();
            let factory: TierDriverTestFactory = Arc::new(move |_| {
                observed_backend_builds.fetch_add(1, Ordering::SeqCst);
                Ok(Box::new(RecordingWarmBackend::new()))
            });
            let peer_calls = Arc::new(Mutex::new(Vec::new()));

            let err = TIER_DRIVER_TEST_FACTORY
                .scope(
                    factory,
                    TIER_MUTATION_TEST_PEERS.scope(
                        vec![FakeTierMutationPeer::boxed(
                            "peer-a",
                            peer_calls.clone(),
                            Ok(PeerTierMutationState::Committed),
                        )],
                        TierConfigMgr::update_candidate_with_config_lock(
                            &manager,
                            store,
                            TierCandidateMutation::Add(Box::new(tier), true),
                        ),
                    ),
                )
                .await
                .expect_err("conflicting canonical and legacy tier names must fail closed");
            let TierConfigUpdateError::Mutation(err) = err else {
                panic!("conflicting {provider} names should fail as a typed mutation error")
            };
            assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
            assert!(err.message.contains("conflicts"), "{}", err.message);
            assert_eq!(backend_builds.load(Ordering::SeqCst), 0, "{provider} backend must not be prepared");
            assert!(lock_unpoisoned(&peer_calls).is_empty(), "{provider} conflict must not prepare peers");
            assert!(manager.read().await.tiers.is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_add_times_out_a_hanging_in_use_check_after_probe_cleanup() {
        let mut mgr = empty_mgr();
        let factory: TierDriverTestFactory = Arc::new(|_| Ok(Box::new(HangingInUseBackend::default())));
        let err = TIER_DRIVER_TEST_FACTORY
            .scope(factory, mgr.add(build_rustfs_tier("COLD-A"), false))
            .await
            .expect_err("a hanging in-use check must not retain the admin lock forever");

        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(err.message.contains("Timed out checking"));
        assert!(mgr.tiers.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn candidate_add_honors_the_caller_validation_deadline() {
        let mut candidate = empty_mgr();
        let factory: TierDriverTestFactory = Arc::new(|_| Ok(Box::new(HangingInUseBackend::default())));
        let deadline = Instant::now() + Duration::from_secs(1);
        let err = {
            let add = TIER_DRIVER_TEST_FACTORY.scope(
                factory,
                apply_tier_candidate_mutation(
                    TierCandidateMutation::Add(Box::new(build_rustfs_tier("COLD-DEADLINE")), false),
                    &mut candidate,
                    deadline,
                ),
            );
            tokio::pin!(add);
            tokio::task::yield_now().await;

            let result = tokio::time::timeout(Duration::from_secs(2), &mut add);
            tokio::pin!(result);
            tokio::time::advance(Duration::from_secs(2) + Duration::from_millis(1)).await;
            result
                .await
                .expect("the add mutation must finish within its caller deadline")
                .expect_err("a hanging in-use check must fail the add mutation")
        };

        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(err.message.contains("Timed out checking"));
        assert!(candidate.tiers.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn candidate_edit_honors_the_caller_validation_deadline() {
        let mut candidate = empty_mgr();
        candidate
            .tiers
            .insert("COLD-DEADLINE".to_string(), build_rustfs_tier("COLD-DEADLINE"));
        let backend = RecordingWarmBackend::new();
        let get_barrier = backend.arm_get_barrier().await;
        let factory = recording_driver_factory(backend.clone(), Arc::new(Mutex::new(Vec::new())));
        let deadline = Instant::now() + Duration::from_secs(1);
        let err = {
            let edit = TIER_DRIVER_TEST_FACTORY.scope(
                factory,
                apply_tier_candidate_mutation(
                    TierCandidateMutation::Edit(
                        "COLD-DEADLINE".to_string(),
                        TierCreds {
                            access_key: "rotated-access".to_string(),
                            secret_key: "rotated-secret".to_string(),
                            ..Default::default()
                        },
                    ),
                    &mut candidate,
                    deadline,
                ),
            );
            tokio::pin!(edit);
            tokio::select! {
                _ = get_barrier.wait_until_paused() => {}
                result = &mut edit => panic!("edit completed before the probe deadline: {result:?}"),
            }

            let result = tokio::time::timeout(Duration::from_secs(2), &mut edit);
            tokio::pin!(result);
            tokio::time::advance(Duration::from_secs(2) + Duration::from_millis(1)).await;
            result
                .await
                .expect("the edit mutation must finish within its caller deadline")
                .expect_err("a hanging in-use check must fail the edit mutation")
        };

        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(err.message.contains("Timed out validating"));
        assert_eq!(backend.object_count().await, 0, "deadline-aware edit must clean up its probe object");
        assert_eq!(
            candidate.tiers["COLD-DEADLINE"]
                .rustfs
                .as_ref()
                .expect("tier payload")
                .access_key,
            "ak"
        );
    }

    // ---- edit -----------------------------------------------------------

    #[tokio::test]
    async fn test_edit_rejects_unknown_tier() {
        let mut mgr = empty_mgr();
        let err = mgr
            .edit("NOPE", TierCreds::default())
            .await
            .expect_err("editing an unknown tier must fail");
        assert_eq!(err.code, ERR_TIER_NOT_FOUND.code);
    }

    #[tokio::test]
    async fn test_edit_rejects_half_static_credentials_for_rustfs() {
        let mut mgr = empty_mgr();
        mgr.tiers.insert("COLD-R".to_string(), build_rustfs_tier("COLD-R"));

        let err = mgr
            .edit(
                "COLD-R",
                TierCreds {
                    access_key: "rotated-access".to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a half-filled static credential pair must be rejected");
        assert_eq!(err.code, ERR_TIER_MISSING_CREDENTIALS.code);
        let rustfs = mgr.tiers["COLD-R"].rustfs.as_ref().expect("original payload should remain");
        assert_eq!(rustfs.access_key, "ak");
        assert_eq!(rustfs.secret_key, "sk");
    }

    #[tokio::test]
    async fn test_edit_rejects_half_static_credentials_for_wasabi() {
        let mut mgr = empty_mgr();
        mgr.tiers.insert("COLD-WASABI".to_string(), build_wasabi_tier("COLD-WASABI"));

        let err = mgr
            .edit(
                "COLD-WASABI",
                TierCreds {
                    secret_key: "rotated-secret".to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a half-filled Wasabi credential pair must be rejected before backend setup");
        assert_eq!(err.code, ERR_TIER_MISSING_CREDENTIALS.code);
    }

    #[tokio::test]
    async fn test_edit_rejects_half_static_credentials_for_minio() {
        let mut mgr = empty_mgr();
        let tier = TierConfig {
            version: "v1".to_string(),
            tier_type: TierType::MinIO,
            name: "COLD-M".to_string(),
            minio: Some(crate::services::tier::tier_config::TierMinIO {
                name: "COLD-M".to_string(),
                endpoint: "https://example-compat.invalid".to_string(),
                access_key: "ak".to_string(),
                secret_key: "sk".to_string(),
                bucket: "bucket-m".to_string(),
                prefix: String::new(),
                region: String::new(),
            }),
            ..Default::default()
        };
        mgr.tiers.insert("COLD-M".to_string(), tier);

        let err = mgr
            .edit(
                "COLD-M",
                TierCreds {
                    access_key: "rotated-access".to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a half-filled static credential pair must be rejected");
        assert_eq!(err.code, ERR_TIER_MISSING_CREDENTIALS.code);
    }

    #[tokio::test]
    async fn test_edit_with_omitted_secret_fields_preserves_real_credentials() {
        let mut mgr = empty_mgr();
        mgr.tiers.insert("COLD-R".to_string(), build_rustfs_tier("COLD-R"));
        let backend = RecordingWarmBackend::new();

        TIER_DRIVER_TEST_FACTORY
            .scope(
                recording_driver_factory(backend.clone(), Arc::new(Mutex::new(Vec::new()))),
                mgr.edit("COLD-R", TierCreds::default()),
            )
            .await
            .expect("an edit with omitted secret fields should validate the preserved credentials");

        let rustfs = mgr.tiers["COLD-R"].rustfs.as_ref().expect("edited payload should remain");
        assert_eq!(rustfs.access_key, "ak");
        assert_eq!(rustfs.secret_key, "sk");
        let operations = backend.op_log().await;
        assert_eq!(operations.len(), 5);
        assert!(matches!(&operations[0], MockWarmOp::Put { .. }));
        assert!(matches!(&operations[1], MockWarmOp::Probe { .. }));
        assert!(matches!(&operations[2], MockWarmOp::Get { .. }));
        assert!(matches!(&operations[3], MockWarmOp::Remove { .. }));
        assert!(matches!(&operations[4], MockWarmOp::Probe { .. }));
    }

    #[tokio::test]
    async fn test_edit_rejects_redaction_placeholder() {
        let mut mgr = empty_mgr();
        mgr.tiers.insert("COLD-R".to_string(), build_rustfs_tier("COLD-R"));

        let err = mgr
            .edit(
                "COLD-R",
                TierCreds {
                    access_key: "rotated-access".to_string(),
                    secret_key: TIER_CREDENTIAL_REDACTED.to_string(),
                    ..Default::default()
                },
            )
            .await
            .expect_err("the API redaction placeholder must never become a real credential");

        assert_eq!(err.code, ERR_TIER_MISSING_CREDENTIALS.code);
        assert_eq!(
            mgr.tiers["COLD-R"]
                .rustfs
                .as_ref()
                .expect("original payload should remain")
                .secret_key,
            "sk"
        );
    }

    #[tokio::test]
    async fn test_edit_rejects_unsupported_s3_role_before_backend_setup() {
        let mut mgr = empty_mgr();
        mgr.tiers.insert("COLD-A".to_string(), build_s3_tier("COLD-A"));

        let err = mgr
            .edit(
                "COLD-A",
                TierCreds {
                    aws_role: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("unsupported role credentials must fail before backend setup");

        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert!(err.message.contains("not supported"));
    }

    #[tokio::test]
    async fn test_edit_gcs_rotates_from_creds_json_and_runs_backend_probe() {
        const OLD_CREDS: &str = r#"{"type":"service_account","project_id":"old-project"}"#;
        const ROTATED_CREDS: &str = r#"{"type":"service_account","project_id":"rotated-project"}"#;
        let mut mgr = empty_mgr();
        mgr.tiers
            .insert("COLD-GCS".to_string(), build_gcs_tier("COLD-GCS", OLD_CREDS));
        let backend = RecordingWarmBackend::new();
        let observed = Arc::new(Mutex::new(Vec::new()));

        TIER_DRIVER_TEST_FACTORY
            .scope(
                recording_driver_factory(backend.clone(), observed.clone()),
                mgr.edit(
                    "COLD-GCS",
                    TierCreds {
                        creds_json: ROTATED_CREDS.as_bytes().to_vec(),
                        ..Default::default()
                    },
                ),
            )
            .await
            .expect("GCS service account rotation should validate through the fake backend");

        assert_eq!(
            mgr.tiers["COLD-GCS"].gcs.as_ref().expect("GCS payload should remain").creds,
            ROTATED_CREDS
        );
        assert_eq!(
            lock_unpoisoned(&observed)[0]
                .gcs
                .as_ref()
                .expect("observed GCS payload should exist")
                .creds,
            ROTATED_CREDS
        );
        let operations = backend.op_log().await;
        assert_eq!(operations.len(), 5);
        assert!(matches!(&operations[0], MockWarmOp::Put { .. }));
        assert!(matches!(&operations[1], MockWarmOp::Probe { .. }));
        assert!(matches!(&operations[2], MockWarmOp::Get { .. }));
        assert!(matches!(&operations[3], MockWarmOp::Remove { .. }));
        assert!(matches!(&operations[4], MockWarmOp::Probe { .. }));
    }

    #[tokio::test]
    async fn test_gcs_madmin_add_wire_normalizes_url_safe_credentials_to_raw_v2_storage() {
        const INITIAL_JSON: &str = r#"{"type":"service_account","project_id":"tier-௿"}"#;
        const INITIAL_MINIO_URL_BASE64: &str = "eyJ0eXBlIjoic2VydmljZV9hY2NvdW50IiwicHJvamVjdF9pZCI6InRpZXIt4K-_In0=";
        const ROTATED_JSON: &str = r#"{"type":"service_account","project_id":"rotated-project"}"#;
        let madmin_wire_add: TierConfig = serde_json::from_value(serde_json::json!({
            "Version": "v1",
            "Type": "gcs",
            "Name": "COLD-GCS",
            "GCS": {
                "Endpoint": "https://storage.googleapis.com/",
                "Creds": INITIAL_MINIO_URL_BASE64,
                "Bucket": "bucket-gcs",
                "Prefix": "prefix-gcs",
                "Region": "",
                "StorageClass": ""
            }
        }))
        .expect("canonical madmin GCS AddTier payload should decode");
        assert_eq!(madmin_wire_add.name, "COLD-GCS");
        assert!(
            madmin_wire_add
                .gcs
                .as_ref()
                .expect("the decoded madmin payload should contain GCS configuration")
                .name
                .is_empty(),
            "canonical madmin GCS has no nested Name field"
        );
        assert_eq!(
            madmin_wire_add
                .gcs
                .as_ref()
                .expect("the decoded madmin payload should contain GCS configuration")
                .creds,
            INITIAL_MINIO_URL_BASE64
        );
        let gcs = madmin_wire_add
            .gcs
            .as_ref()
            .expect("the decoded madmin payload should contain GCS configuration");
        assert_eq!(gcs.endpoint, "https://storage.googleapis.com/");
        assert_eq!(gcs.bucket, "bucket-gcs");
        assert_eq!(gcs.prefix, "prefix-gcs");
        assert!(gcs.region.is_empty());
        assert!(gcs.storage_class.is_empty());
        let rustfs_output = serde_json::to_value(&madmin_wire_add).expect("GCS config should serialize");
        assert_eq!(rustfs_output["type"], "gcs");
        assert!(rustfs_output.get("Type").is_none());
        assert!(rustfs_output.get("GCS").is_none());
        assert!(rustfs_output.get("Name").is_none());
        assert!(rustfs_output["gcs"].get("Creds").is_none());
        assert_eq!(rustfs_output["gcs"]["creds"], INITIAL_MINIO_URL_BASE64);
        let mut mgr = empty_mgr();
        let backend = RecordingWarmBackend::new();
        let observed = Arc::new(Mutex::new(Vec::new()));
        TIER_DRIVER_TEST_FACTORY
            .scope(
                recording_driver_factory(backend.clone(), observed.clone()),
                mgr.add(madmin_wire_add, true),
            )
            .await
            .expect("GCS add should normalize madmin URL-safe credentials before validation");

        assert_eq!(
            lock_unpoisoned(&observed)[0]
                .gcs
                .as_ref()
                .expect("the backend should receive a GCS payload")
                .creds,
            INITIAL_JSON
        );
        assert_eq!(
            mgr.tiers["COLD-GCS"]
                .gcs
                .as_ref()
                .expect("the added GCS payload should exist")
                .creds,
            INITIAL_JSON
        );
        assert_eq!(mgr.tiers["COLD-GCS"].name, "COLD-GCS");
        assert_eq!(
            mgr.tiers["COLD-GCS"]
                .gcs
                .as_ref()
                .expect("the added GCS payload should exist")
                .name,
            "COLD-GCS"
        );

        let blob = encode_external_tiering_config_blob(&mgr).expect("GCS add should persist externally");
        let external: ExternalTierConfigMgr = rmp_serde::from_slice(&blob[4..]).expect("external GCS payload should decode");
        assert_eq!(
            external.tiers["COLD-GCS"]
                .gcs
                .as_ref()
                .expect("external GCS payload should exist")
                .creds,
            INITIAL_JSON
        );
        let mut loaded = decode_external_tiering_config_blob(&blob).expect("persisted GCS tier should load");
        assert_eq!(
            loaded.tiers["COLD-GCS"]
                .gcs
                .as_ref()
                .expect("loaded GCS payload should exist")
                .creds,
            INITIAL_JSON
        );

        TIER_DRIVER_TEST_FACTORY
            .scope(
                recording_driver_factory(backend, Arc::new(Mutex::new(Vec::new()))),
                loaded.edit(
                    "COLD-GCS",
                    TierCreds {
                        creds_json: ROTATED_JSON.as_bytes().to_vec(),
                        ..Default::default()
                    },
                ),
            )
            .await
            .expect("GCS edit should accept raw madmin credential bytes");
        assert_eq!(
            loaded.tiers["COLD-GCS"]
                .gcs
                .as_ref()
                .expect("edited GCS payload should exist")
                .creds,
            ROTATED_JSON
        );
        let rotated_blob = encode_external_tiering_config_blob(&loaded).expect("edited GCS tier should persist externally");
        assert_eq!(
            rmp_serde::from_slice::<ExternalTierConfigMgr>(&rotated_blob[4..])
                .expect("rotated external tier should exist")
                .tiers["COLD-GCS"]
                .gcs
                .as_ref()
                .expect("rotated external GCS payload should exist")
                .creds,
            ROTATED_JSON
        );
        let reloaded = decode_external_tiering_config_blob(&rotated_blob).expect("edited GCS tier should reload");
        assert_eq!(
            reloaded.tiers["COLD-GCS"]
                .gcs
                .as_ref()
                .expect("reloaded GCS payload should exist")
                .creds,
            ROTATED_JSON
        );
    }

    // ---- remove ---------------------------------------------------------

    #[tokio::test]
    async fn test_remove_unknown_tier_is_idempotent() {
        let mut mgr = empty_mgr();
        // Unknown tier: get_driver returns NotFound, which `remove` swallows.
        mgr.remove("NOPE", false).await.expect("removing an unknown tier is a no-op");
    }

    #[tokio::test]
    async fn test_remove_rejects_backend_in_use() {
        let mut mgr = empty_mgr();
        inject_mock(
            &mut mgr,
            "COLD-A",
            build_s3_tier("COLD-A"),
            MockWarmBackend {
                in_use_value: Some(true),
                healthy: true,
                probe_present: AtomicBool::new(false),
            },
        );

        let err = mgr
            .remove("COLD-A", false)
            .await
            .expect_err("in-use backend must not be removed");
        assert_eq!(err.code, ERR_TIER_BACKEND_NOT_EMPTY.code);
        assert!(mgr.tiers.contains_key("COLD-A"), "tier must survive a rejected remove");
        assert!(mgr.driver_cache.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn test_remove_succeeds_when_backend_empty() {
        let mut mgr = empty_mgr();
        inject_mock(
            &mut mgr,
            "COLD-A",
            build_s3_tier("COLD-A"),
            MockWarmBackend {
                in_use_value: Some(false),
                healthy: true,
                probe_present: AtomicBool::new(false),
            },
        );

        mgr.remove("COLD-A", false).await.expect("empty backend can be removed");
        assert!(!mgr.tiers.contains_key("COLD-A"));
        assert!(!mgr.driver_cache.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn test_remove_force_skips_in_use_probe() {
        let mut mgr = empty_mgr();
        // in_use_value: None would error if probed; force=true must skip it.
        inject_mock(
            &mut mgr,
            "COLD-A",
            build_s3_tier("COLD-A"),
            MockWarmBackend {
                in_use_value: None,
                healthy: true,
                probe_present: AtomicBool::new(false),
            },
        );

        mgr.remove("COLD-A", true).await.expect("force remove must not probe in_use");
        assert!(!mgr.tiers.contains_key("COLD-A"));
        assert!(!mgr.driver_cache.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn persisted_remove_preserves_force_semantics() {
        let mut candidate = empty_mgr();
        inject_mock(
            &mut candidate,
            "COLD-A",
            build_s3_tier("COLD-A"),
            MockWarmBackend {
                in_use_value: None,
                healthy: true,
                probe_present: AtomicBool::new(false),
            },
        );
        let mutation = TierCandidateMutation::Remove("COLD-A".to_string(), true);
        mutation
            .apply(&mut candidate, None)
            .await
            .expect("force remove must skip in-use probing");
        assert!(!candidate.tiers.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn persisted_remove_nonforce_preserves_backend_in_use_error() {
        let mut candidate = empty_mgr();
        inject_mock(
            &mut candidate,
            "COLD-A",
            build_s3_tier("COLD-A"),
            MockWarmBackend {
                in_use_value: Some(true),
                healthy: true,
                probe_present: AtomicBool::new(false),
            },
        );
        let err = TierCandidateMutation::Remove("COLD-A".to_string(), false)
            .apply(&mut candidate, None)
            .await
            .expect_err("non-force remove must reject an in-use backend");
        assert_eq!(err.code, ERR_TIER_BACKEND_NOT_EMPTY.code);
        assert!(candidate.tiers.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn clear_preserves_force_and_nonforce_semantics() {
        let mut forced = empty_mgr();
        inject_mock(
            &mut forced,
            "COLD-A",
            build_s3_tier("COLD-A"),
            MockWarmBackend {
                in_use_value: None,
                healthy: true,
                probe_present: AtomicBool::new(false),
            },
        );
        forced.clear_tier(true).await.expect("force clear must skip in-use probing");
        assert!(forced.tiers.is_empty());

        let mut guarded = empty_mgr();
        inject_mock(
            &mut guarded,
            "COLD-A",
            build_s3_tier("COLD-A"),
            MockWarmBackend {
                in_use_value: Some(true),
                healthy: true,
                probe_present: AtomicBool::new(false),
            },
        );
        let err = guarded
            .clear_tier(false)
            .await
            .expect_err("non-force clear must reject an in-use backend");
        assert_eq!(err.code, ERR_TIER_BACKEND_NOT_EMPTY.code);
        assert!(guarded.tiers.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn test_remove_reports_probe_error() {
        let mut mgr = empty_mgr();
        inject_mock(
            &mut mgr,
            "COLD-A",
            build_s3_tier("COLD-A"),
            MockWarmBackend {
                in_use_value: None,
                healthy: true,
                probe_present: AtomicBool::new(false),
            },
        );

        let err = mgr
            .remove("COLD-A", false)
            .await
            .expect_err("a failed in_use probe must surface as an error");
        assert_eq!(err.code, ERR_TIER_PERM_ERR.code);
        assert!(mgr.tiers.contains_key("COLD-A"), "tier must survive a failed probe");
    }

    // ---- verify ---------------------------------------------------------

    #[tokio::test]
    async fn test_verify_unknown_tier_errors() {
        let mut mgr = empty_mgr();
        let err = mgr.verify("NOPE").await.expect_err("verifying an unknown tier must fail");
        assert!(err.to_string().contains("not found"), "{err}");
    }

    #[tokio::test]
    async fn test_verify_succeeds_with_healthy_backend() {
        let mut mgr = empty_mgr();
        inject_mock(
            &mut mgr,
            "COLD-A",
            build_s3_tier("COLD-A"),
            MockWarmBackend {
                in_use_value: Some(false),
                healthy: true,
                probe_present: AtomicBool::new(false),
            },
        );

        mgr.verify("COLD-A").await.expect("healthy backend must verify");
    }

    #[tokio::test]
    async fn test_verify_reports_backend_failure() {
        let mut mgr = empty_mgr();
        inject_mock(
            &mut mgr,
            "COLD-A",
            build_s3_tier("COLD-A"),
            MockWarmBackend {
                in_use_value: Some(false),
                healthy: false,
                probe_present: AtomicBool::new(false),
            },
        );

        mgr.verify("COLD-A")
            .await
            .expect_err("an unhealthy backend must fail verification");
    }

    #[tokio::test]
    async fn shared_backend_proxy_forwards_validation_hooks() {
        let unhealthy: SharedWarmBackend = Arc::new(MockWarmBackend {
            in_use_value: Some(false),
            healthy: false,
            probe_present: AtomicBool::new(false),
        });
        let proxy = SharedWarmBackendProxy(unhealthy);
        let err = proxy.validate().await.expect_err("proxy must forward backend validation");
        assert_eq!(err.to_string(), "mock validation failed");

        let healthy: SharedWarmBackend = Arc::new(MockWarmBackend {
            in_use_value: Some(false),
            healthy: true,
            probe_present: AtomicBool::new(false),
        });
        let proxy = SharedWarmBackendProxy(healthy);
        let err = proxy
            .validate_remote_version_id("unsupported-version")
            .expect_err("proxy must forward remote version validation");
        assert_eq!(err.to_string(), "mock remote version rejected");
        let err = proxy
            .remove_exact("remote-object", "exact-only")
            .await
            .expect_err("proxy must forward exact-version cleanup");
        assert_eq!(err.to_string(), "mock exact remove forwarded");
    }

    // ---- pure query helpers --------------------------------------------

    #[test]
    fn test_query_helpers_reflect_membership() {
        let mut mgr = empty_mgr();
        assert!(mgr.empty());
        assert!(!mgr.is_tier_valid("COLD-A"));
        assert_eq!(mgr.tier_type("COLD-A"), "internal");
        assert!(mgr.get("COLD-A").is_none());

        mgr.tiers.insert("COLD-A".to_string(), build_s3_tier("COLD-A"));

        assert!(!mgr.empty());
        assert!(mgr.is_tier_valid("COLD-A"));
        let (ty, in_use) = mgr.is_tier_name_in_use("COLD-A");
        assert!(in_use);
        assert_eq!(ty.as_lowercase(), "s3");
        assert_eq!(mgr.tier_type("COLD-A"), "s3");
        assert_eq!(mgr.list_tiers().len(), 1);
        assert!(mgr.get("COLD-A").is_some());
    }

    // ---- persistence: encode/decode and legacy migration ---------------

    #[test]
    fn test_marshal_unmarshal_json_roundtrip_preserves_tier() {
        let mut mgr = empty_mgr();
        mgr.tiers.insert("COLD-A".to_string(), build_s3_tier("COLD-A"));

        let bytes = mgr.marshal().expect("marshal should succeed");
        let decoded = TierConfigMgr::unmarshal(&bytes).expect("unmarshal should succeed");

        let tier = decoded.tiers.get("COLD-A").expect("tier survives json roundtrip");
        assert_eq!(tier.tier_type.as_lowercase(), "s3");
        let s3 = tier.s3.as_ref().expect("s3 payload survives");
        assert_eq!(s3.bucket, "bucket-a");
        // Internal serialization preserves credentials; only explicit admin views redact them.
        assert_eq!(s3.secret_key, "sk");
    }

    #[test]
    fn test_external_blob_roundtrip_azure_maps_account_fields() {
        let mut mgr = empty_mgr();
        mgr.tiers.insert(
            "COLD-AZ".to_string(),
            TierConfig {
                version: "v1".to_string(),
                tier_type: TierType::Azure,
                name: "COLD-AZ".to_string(),
                azure: Some(crate::services::tier::tier_config::TierAzure {
                    name: "COLD-AZ".to_string(),
                    endpoint: "https://example-azure.invalid".to_string(),
                    access_key: "account-name".to_string(),
                    secret_key: "account-key".to_string(),
                    bucket: "container".to_string(),
                    prefix: "p".to_string(),
                    region: "eastus".to_string(),
                    storage_class: "HOT".to_string(),
                    sp_auth: crate::services::tier::tier_config::ServicePrincipalAuth {
                        tenant_id: "tenant".to_string(),
                        client_id: "client".to_string(),
                        client_secret: "secret".to_string(),
                    },
                }),
                ..Default::default()
            },
        );

        let bytes = encode_external_tiering_config_blob(&mgr).expect("encode azure tier");
        let decoded = decode_external_tiering_config_blob(&bytes).expect("decode azure tier");
        let tier = decoded.tiers.get("COLD-AZ").expect("azure tier survives roundtrip");
        assert_eq!(tier.tier_type.as_lowercase(), "azure");
        let az = tier.azure.as_ref().expect("azure payload survives");
        // AccountName/AccountKey in the on-disk format map back to access/secret.
        assert_eq!(az.access_key, "account-name");
        assert_eq!(az.secret_key, "account-key");
        assert_eq!(az.sp_auth.tenant_id, "tenant");
        assert_eq!(az.sp_auth.client_secret, "secret");
    }

    #[test]
    fn test_external_blob_gcs_writer_stays_raw_for_v2_old_readers() {
        const RAW_JSON: &str = r#"{"type":"service_account","project_id":"tier-௿"}"#;
        let mut mgr = empty_mgr();
        mgr.tiers.insert(
            "COLD-G".to_string(),
            TierConfig {
                version: "v1".to_string(),
                tier_type: TierType::GCS,
                name: "COLD-G".to_string(),
                gcs: Some(crate::services::tier::tier_config::TierGCS {
                    name: "COLD-G".to_string(),
                    endpoint: "https://storage.googleapis.com".to_string(),
                    creds: RAW_JSON.to_string(),
                    bucket: "gbucket".to_string(),
                    prefix: "gp".to_string(),
                    region: "us".to_string(),
                    storage_class: "NEARLINE".to_string(),
                }),
                ..Default::default()
            },
        );

        let bytes = encode_external_tiering_config_blob(&mgr).expect("encode gcs tier");
        assert_eq!(&bytes[0..2], &TIER_CONFIG_FORMAT.to_le_bytes());
        assert_eq!(&bytes[2..4], &TIER_CONFIG_VERSION.to_le_bytes());
        let external: ExternalTierConfigMgr = rmp_serde::from_slice(&bytes[4..]).expect("external GCS payload should decode");
        assert_eq!(
            external.tiers["COLD-G"]
                .gcs
                .as_ref()
                .expect("external GCS payload should exist")
                .creds,
            RAW_JSON
        );
        serde_json::from_str::<serde_json::Value>(
            &external.tiers["COLD-G"]
                .gcs
                .as_ref()
                .expect("external GCS payload should exist")
                .creds,
        )
        .expect("the baseline RustFS v2 reader must receive raw GCS credential JSON");
        let decoded = decode_external_tiering_config_blob(&bytes).expect("decode gcs tier");
        let tier = decoded.tiers.get("COLD-G").expect("gcs tier survives roundtrip");
        assert_eq!(tier.tier_type.as_lowercase(), "gcs");
        assert_eq!(tier.gcs.as_ref().expect("gcs payload survives").creds, RAW_JSON);

        let rewritten = encode_external_tiering_config_blob(&decoded).expect("decoded GCS tier should remain v2-compatible");
        let rewritten: ExternalTierConfigMgr =
            rmp_serde::from_slice(&rewritten[4..]).expect("rewritten external GCS payload should decode");
        assert_eq!(
            rewritten.tiers["COLD-G"]
                .gcs
                .as_ref()
                .expect("rewritten external GCS payload should exist")
                .creds,
            RAW_JSON
        );
    }

    fn decode_hex_fixture(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len() % 2, 0, "hex fixture must contain complete bytes");
        hex.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                let pair = std::str::from_utf8(pair).expect("hex fixture should be ASCII");
                u8::from_str_radix(pair, 16).expect("hex fixture should contain only hexadecimal digits")
            })
            .collect()
    }

    #[test]
    fn test_external_blob_decodes_fixed_minio_gcs_fixture_and_rewrites_raw_v2_credentials() {
        const RAW_JSON: &str = r#"{"type":"service_account","project_id":"tier-௿"}"#;
        // Produced by MinIO RELEASE.2025-10-15T17-29-55Z with madmin-go/v3
        // v3.0.109. Keeping the bytes fixed prevents this compatibility test
        // from accidentally validating RustFS against its own serializer.
        const MINIO_GCS_BLOB_HEX: &str = concat!(
            "0100020081a5546965727381a6434f4c442d4787a756657273696f6ea27631a45479706503a44e616d65a6434f4c442d47",
            "a25333c0a5417a757265c0a347435386a8456e64706f696e74bf68747470733a2f2f73746f726167652e676f6f676c656170",
            "69732e636f6d2fa54372656473d94465794a306558426c496a6f6963325679646d6c6a5a56396859324e7664573530496977",
            "6963484a76616d566a644639705a434936496e52705a584974344b2d5f496e303da64275636b6574a7676275636b6574a650",
            "7265666978a26770a6526567696f6ea27573ac53746f72616765436c617373a84e4541524c494e45a54d696e494fc0"
        );
        const MINIO_GCS_BLOB_SHA256_HEX: &str = "bee1d4822d4936bc6f764c284381596d16f2096f40671b25500aca3b710ceac8";
        let fixture = decode_hex_fixture(MINIO_GCS_BLOB_HEX);
        assert_eq!(fixture.len(), 246);
        assert_eq!(
            Sha256::digest(&fixture).as_slice(),
            decode_hex_fixture(MINIO_GCS_BLOB_SHA256_HEX).as_slice(),
            "fixed foreign fixture digest changed"
        );

        let decoded = decode_external_tiering_config_blob(&fixture)
            .expect("the new reader should accept MinIO URL-safe-base64 GCS credentials");
        let gcs = decoded.tiers["COLD-G"]
            .gcs
            .as_ref()
            .expect("decoded MinIO GCS payload should exist");
        assert_eq!(gcs.endpoint, "https://storage.googleapis.com/");
        assert_eq!(gcs.bucket, "gbucket");
        assert_eq!(gcs.prefix, "gp");
        assert_eq!(gcs.region, "us");
        assert_eq!(gcs.storage_class, "NEARLINE");
        assert_eq!(gcs.creds, RAW_JSON);

        let rewritten = encode_external_tiering_config_blob(&decoded)
            .expect("MinIO credentials should rewrite in the existing RustFS v2 raw format");
        let rewritten: ExternalTierConfigMgr =
            rmp_serde::from_slice(&rewritten[4..]).expect("rewritten MinIO fixture should decode");
        assert_eq!(
            rewritten.tiers["COLD-G"]
                .gcs
                .as_ref()
                .expect("rewritten MinIO GCS payload should exist")
                .creds,
            RAW_JSON
        );
    }

    #[test]
    fn external_gcs_credentials_accept_all_base64_alphabets_and_padding_modes() {
        let raw = r#"{"type":"service_account","project_id":"tier-🚀"}"#;
        for encoder in [
            base64_simd::STANDARD,
            base64_simd::STANDARD_NO_PAD,
            base64_simd::URL_SAFE,
            base64_simd::URL_SAFE_NO_PAD,
        ] {
            let encoded = encoder.encode_to_string(raw.as_bytes());
            assert_eq!(
                decode_external_gcs_credentials(&encoded).expect("supported GCS encoding should decode"),
                raw
            );
        }
    }

    // `TierConfigMgr` intentionally does not derive `Debug` (it holds live
    // driver handles), so `Result::expect_err` cannot be used on decode
    // results. Extract the error explicitly instead.
    fn expect_decode_err(data: &[u8]) -> std::io::Error {
        match decode_external_tiering_config_blob(data) {
            Ok(_) => panic!("expected decode to fail"),
            Err(err) => err,
        }
    }

    #[test]
    fn test_external_blob_rejects_truncated_data() {
        let err = expect_decode_err(&[0u8; 3]);
        assert!(err.to_string().contains("no data"), "{err}");
    }

    #[test]
    fn test_external_blob_rejects_unknown_format() {
        // Valid length, but a format word the decoder does not recognise.
        let mut data = Vec::new();
        data.extend_from_slice(&99u16.to_le_bytes());
        data.extend_from_slice(&TIER_CONFIG_VERSION.to_le_bytes());
        data.extend_from_slice(&[0u8; 8]);
        let err = expect_decode_err(&data);
        assert!(err.to_string().contains("unknown format"), "{err}");
    }

    #[test]
    fn test_external_blob_rejects_unknown_version() {
        let mut data = Vec::new();
        data.extend_from_slice(&TIER_CONFIG_FORMAT.to_le_bytes());
        data.extend_from_slice(&99u16.to_le_bytes());
        data.extend_from_slice(&[0u8; 8]);
        let err = expect_decode_err(&data);
        assert!(err.to_string().contains("unknown version"), "{err}");
    }

    #[test]
    fn test_external_blob_ignores_unknown_fields_at_all_legacy_layers() {
        let payload = serde_json::json!({
            "Tiers": {
                "COLD-A": {
                    "Version": "v1",
                    "Type": EXTERNAL_TIER_TYPE_S3,
                    "Name": "COLD-A",
                    "S3": {
                        "Endpoint": "https://example.invalid",
                        "AccessKey": "ak",
                        "SecretKey": "sk",
                        "Bucket": "bucket",
                        "Prefix": "objects",
                        "UnknownS3": true
                    },
                    "Azure": {
                        "SPAuth": { "UnknownSPAuth": true },
                        "UnknownAzure": true
                    },
                    "GCS": { "UnknownGCS": true },
                    "MinIO": { "UnknownMinIO": true },
                    "UnknownTier": true
                }
            },
            "UnknownManager": true
        });
        let encoded = rmp_serde::to_vec(&payload).expect("test payload should encode");
        let mut data = Vec::with_capacity(4 + encoded.len());
        data.extend_from_slice(&TIER_CONFIG_FORMAT.to_le_bytes());
        data.extend_from_slice(&TIER_CONFIG_VERSION.to_le_bytes());
        data.extend_from_slice(&encoded);

        let decoded = decode_external_tiering_config_blob(&data).expect("legacy unknown fields must remain forward-compatible");
        let s3 = decoded.tiers["COLD-A"].s3.as_ref().expect("s3 payload should decode");
        assert_eq!(s3.prefix, "objects");
    }

    #[test]
    fn test_external_blob_decodes_minio_compatible_fixture() {
        let payload = serde_json::json!({
            "Tiers": {
                "COLD-M": {
                    "Version": "v1",
                    "Type": EXTERNAL_TIER_TYPE_MINIO,
                    "Name": "COLD-M",
                    "MinIO": {
                        "Endpoint": "https://minio.example.invalid",
                        "AccessKey": "ak",
                        "SecretKey": "sk",
                        "Bucket": "archive",
                        "Prefix": "objects/",
                        "Region": "us-east-1"
                    }
                }
            }
        });
        let encoded = rmp_serde::to_vec(&payload).expect("fixture payload should encode");
        let mut data = Vec::with_capacity(4 + encoded.len());
        data.extend_from_slice(&TIER_CONFIG_FORMAT.to_le_bytes());
        data.extend_from_slice(&TIER_CONFIG_VERSION.to_le_bytes());
        data.extend_from_slice(&encoded);

        let decoded = decode_external_tiering_config_blob(&data).expect("MinIO-compatible fixture must decode");
        let minio = decoded.tiers["COLD-M"].minio.as_ref().expect("MinIO payload should decode");
        assert_eq!(minio.endpoint, "https://minio.example.invalid");
        assert_eq!(minio.bucket, "archive");
        assert_eq!(minio.prefix, "objects/");
    }

    #[test]
    fn test_external_blob_accepts_legacy_v1_version_word() {
        // The decoder must keep accepting the historical v1 version word, not
        // just the current TIER_CONFIG_VERSION, so old on-disk configs load.
        let mut mgr = empty_mgr();
        mgr.tiers.insert("COLD-A".to_string(), build_s3_tier("COLD-A"));
        let current = encode_external_tiering_config_blob(&mgr).expect("encode");

        // Rewrite the version word (bytes 2..4) from VERSION to V1.
        let mut legacy = current.to_vec();
        legacy[2..4].copy_from_slice(&TIER_CONFIG_V1.to_le_bytes());

        let decoded = decode_external_tiering_config_blob(&legacy).expect("v1 version word must decode");
        assert!(decoded.tiers.contains_key("COLD-A"));
    }

    #[test]
    fn test_encode_external_blob_errors_on_missing_payload() {
        let mut mgr = empty_mgr();
        mgr.tiers.insert(
            "COLD-A".to_string(),
            TierConfig {
                version: "v1".to_string(),
                tier_type: TierType::S3,
                name: "COLD-A".to_string(),
                s3: None,
                ..Default::default()
            },
        );
        let err = encode_external_tiering_config_blob(&mgr).expect_err("missing payload must fail encode");
        assert!(err.to_string().contains("missing s3 backend payload"), "{err}");
    }

    #[derive(Clone)]
    struct LeaseTestBackend {
        id: &'static str,
        calls: Arc<std::sync::Mutex<Vec<&'static str>>>,
        probe_present: Arc<AtomicBool>,
        remove_started: Option<Arc<tokio::sync::Notify>>,
        remove_release: Option<Arc<tokio::sync::Semaphore>>,
        backend_in_use: bool,
        pending_in_use: bool,
        panic_in_use: bool,
    }

    impl LeaseTestBackend {
        fn ready(id: &'static str) -> Self {
            Self {
                id,
                calls: Arc::new(std::sync::Mutex::new(Vec::new())),
                probe_present: Arc::new(AtomicBool::new(false)),
                remove_started: None,
                remove_release: None,
                backend_in_use: false,
                pending_in_use: false,
                panic_in_use: false,
            }
        }

        fn blocking(id: &'static str) -> Self {
            Self {
                remove_started: Some(Arc::new(tokio::sync::Notify::new())),
                remove_release: Some(Arc::new(tokio::sync::Semaphore::new(0))),
                ..Self::ready(id)
            }
        }

        fn calls(&self) -> Vec<&'static str> {
            self.calls.lock().expect("lease test call log should not poison").clone()
        }

        fn panicking_in_use(id: &'static str) -> Self {
            Self {
                panic_in_use: true,
                ..Self::ready(id)
            }
        }

        fn in_use(id: &'static str) -> Self {
            Self {
                backend_in_use: true,
                ..Self::ready(id)
            }
        }

        fn pending_in_use(id: &'static str) -> Self {
            Self {
                pending_in_use: true,
                ..Self::ready(id)
            }
        }
    }

    #[async_trait::async_trait]
    impl WarmBackend for LeaseTestBackend {
        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> io::Result<String> {
            self.probe_present.store(true, Ordering::SeqCst);
            Ok(self.id.to_string())
        }

        async fn put_with_meta(
            &self,
            object: &str,
            r: ReaderImpl,
            length: i64,
            _meta: HashMap<String, String>,
        ) -> io::Result<String> {
            self.put(object, r, length).await
        }

        async fn get(&self, _object: &str, _rv: &str, _opts: WarmBackendGetOpts) -> io::Result<ReadCloser> {
            Ok(BufReader::new(Cursor::new(b"RustFS".to_vec())))
        }

        async fn remove(&self, _object: &str, _rv: &str) -> io::Result<()> {
            self.calls
                .lock()
                .expect("lease test call log should not poison")
                .push(self.id);
            if let Some(started) = &self.remove_started {
                started.notify_one();
            }
            if let Some(release) = &self.remove_release {
                release
                    .acquire()
                    .await
                    .expect("lease test release semaphore should stay open")
                    .forget();
            }
            self.probe_present.store(false, Ordering::SeqCst);
            Ok(())
        }

        async fn probe_transition_candidate(&self, _object: &str) -> io::Result<TransitionCandidateProbe> {
            if self.probe_present.load(Ordering::SeqCst) {
                Ok(TransitionCandidateProbe::VersionedPresent(self.id.to_string()))
            } else {
                Ok(TransitionCandidateProbe::Missing)
            }
        }

        async fn in_use(&self) -> io::Result<bool> {
            if self.panic_in_use {
                panic!("simulated tier backend in-use panic");
            }
            if self.pending_in_use {
                std::future::pending::<()>().await;
            }
            Ok(self.backend_in_use)
        }
    }

    fn install_lease_backend(manager: &mut TierConfigMgr, tier_name: &str, backend: LeaseTestBackend) {
        manager.tiers.insert(tier_name.to_string(), build_rustfs_tier(tier_name));
        manager
            .replace_driver(tier_name, Box::new(backend))
            .expect("test driver generation should install");
    }

    fn prepared_remove_intent(tier_name: &str, mutation_id: uuid::Uuid) -> TierMutationIntent {
        TierMutationIntent {
            mutation_id,
            revision: 1,
            kind: TierMutationIntentKind::Remove,
            state: TierMutationIntentState::Prepared,
            old_config_etag: Some("etag-old".to_string()),
            committed_config_etag: None,
            candidate_digest: [1; 32],
            affected_targets: vec![TierMutationIntentTarget {
                tier_name: tier_name.to_string(),
                old_backend_identity: Some([2; 32]),
                new_backend_identity: None,
            }],
            expires_at_unix_nanos: 1,
        }
    }

    fn committed_remove_intent(tier_name: &str, mutation_id: uuid::Uuid, etag: &str) -> TierMutationIntent {
        let mut intent = prepared_remove_intent(tier_name, mutation_id);
        intent
            .advance(TierMutationIntentState::Committed, Some(etag.to_string()))
            .expect("fixture intent should commit");
        intent
    }

    struct FakeTierMutationPeer {
        label: &'static str,
        calls: Arc<Mutex<Vec<String>>>,
        captured_prepares: Option<Arc<Mutex<Vec<TierMutationIntent>>>>,
        prepare: std::result::Result<PeerTierMutationState, &'static str>,
        prepare_definitely_rejected: bool,
        commit: std::result::Result<PeerTierMutationState, &'static str>,
        abort: std::result::Result<PeerTierMutationState, &'static str>,
    }

    impl FakeTierMutationPeer {
        fn boxed(
            label: &'static str,
            calls: Arc<Mutex<Vec<String>>>,
            commit: std::result::Result<PeerTierMutationState, &'static str>,
        ) -> Arc<dyn TierMutationPeer> {
            Self::boxed_with_prepare_commit(label, calls, Ok(PeerTierMutationState::Prepared), commit)
        }

        fn boxed_with_prepare_commit(
            label: &'static str,
            calls: Arc<Mutex<Vec<String>>>,
            prepare: std::result::Result<PeerTierMutationState, &'static str>,
            commit: std::result::Result<PeerTierMutationState, &'static str>,
        ) -> Arc<dyn TierMutationPeer> {
            Arc::new(Self {
                label,
                calls,
                captured_prepares: None,
                prepare,
                prepare_definitely_rejected: false,
                commit,
                abort: Ok(PeerTierMutationState::Aborted),
            })
        }

        fn boxed_capturing_prepare(
            label: &'static str,
            calls: Arc<Mutex<Vec<String>>>,
            captured_prepares: Arc<Mutex<Vec<TierMutationIntent>>>,
        ) -> Arc<dyn TierMutationPeer> {
            Arc::new(Self {
                label,
                calls,
                captured_prepares: Some(captured_prepares),
                prepare: Ok(PeerTierMutationState::Prepared),
                prepare_definitely_rejected: false,
                commit: Ok(PeerTierMutationState::Committed),
                abort: Ok(PeerTierMutationState::Aborted),
            })
        }

        fn record(&self, call: String) {
            lock_unpoisoned(&self.calls).push(call);
        }
    }

    #[async_trait::async_trait]
    impl TierMutationPeer for FakeTierMutationPeer {
        fn peer_label(&self) -> String {
            self.label.to_string()
        }

        async fn prepare_tier_mutation(
            &self,
            mutation_id: uuid::Uuid,
            canonical_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            if let Some(captured_prepares) = self.captured_prepares.as_ref() {
                let intent = TierMutationIntent::decode(mutation_id, &canonical_payload).map_err(Error::other)?;
                lock_unpoisoned(captured_prepares).push(intent);
            }
            self.record(format!("{}:prepare:{}:{}", self.label, mutation_id, canonical_payload.len()));
            match self.prepare {
                Ok(state) => Ok(state),
                Err(message) if self.prepare_definitely_rejected => Err(
                    crate::cluster::rpc::peer_rest_client::test_tier_mutation_definitely_rejected_error(message),
                ),
                Err(message) => Err(Error::other(message)),
            }
        }

        async fn commit_tier_mutation(&self, mutation_id: uuid::Uuid, canonical_payload: Bytes) -> Result<PeerTierMutationState> {
            self.record(format!(
                "{}:commit:{}:{}",
                self.label,
                mutation_id,
                String::from_utf8_lossy(canonical_payload.as_ref())
            ));
            self.commit.map_err(Error::other)
        }

        async fn abort_tier_mutation(
            &self,
            mutation_id: uuid::Uuid,
            canonical_prepare_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            let prepared = TierMutationIntent::decode(mutation_id, &canonical_prepare_payload).map_err(Error::other)?;
            if prepared.state != TierMutationIntentState::Prepared || prepared.revision != 1 {
                return Err(Error::other("test peer requires the original revision-1 Prepared abort payload"));
            }
            self.record(format!("{}:abort:{}", self.label, mutation_id));
            self.abort.map_err(Error::other)
        }
    }

    struct ConcurrencyTrackingTierMutationPeer {
        label: &'static str,
        calls: Arc<Mutex<Vec<String>>>,
        active: Arc<AtomicUsize>,
        max_active: Arc<AtomicUsize>,
    }

    struct BlockingCommitTierMutationPeer {
        started: Arc<Notify>,
        release: Arc<tokio::sync::Semaphore>,
    }

    struct BlockingPrepareTierMutationPeer {
        started: Arc<Notify>,
        release: Arc<tokio::sync::Semaphore>,
        calls: Arc<Mutex<Vec<String>>>,
    }

    struct HangingPrepareTierMutationPeer {
        calls: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait::async_trait]
    impl TierMutationPeer for BlockingCommitTierMutationPeer {
        fn peer_label(&self) -> String {
            "blocking-peer".to_string()
        }

        async fn prepare_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            Ok(PeerTierMutationState::Prepared)
        }

        async fn commit_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            self.started.notify_one();
            self.release
                .acquire()
                .await
                .expect("blocking commit test semaphore should stay open")
                .forget();
            Ok(PeerTierMutationState::Committed)
        }

        async fn abort_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_prepare_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            Ok(PeerTierMutationState::Aborted)
        }
    }

    #[async_trait::async_trait]
    impl TierMutationPeer for BlockingPrepareTierMutationPeer {
        fn peer_label(&self) -> String {
            "blocking-prepare-peer".to_string()
        }

        async fn prepare_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            lock_unpoisoned(&self.calls).push("prepare".to_string());
            self.started.notify_one();
            self.release
                .acquire()
                .await
                .expect("blocking prepare test semaphore should stay open")
                .forget();
            Ok(PeerTierMutationState::Prepared)
        }

        async fn commit_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            lock_unpoisoned(&self.calls).push("commit".to_string());
            Ok(PeerTierMutationState::Committed)
        }

        async fn abort_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_prepare_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            lock_unpoisoned(&self.calls).push("abort".to_string());
            Ok(PeerTierMutationState::Aborted)
        }
    }

    #[async_trait::async_trait]
    impl TierMutationPeer for HangingPrepareTierMutationPeer {
        fn peer_label(&self) -> String {
            "hanging-peer".to_string()
        }

        async fn prepare_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            lock_unpoisoned(&self.calls).push("hanging-peer:prepare".to_string());
            std::future::pending().await
        }

        async fn commit_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            Ok(PeerTierMutationState::Committed)
        }

        async fn abort_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_prepare_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            lock_unpoisoned(&self.calls).push("hanging-peer:abort".to_string());
            Ok(PeerTierMutationState::Aborted)
        }
    }

    impl ConcurrencyTrackingTierMutationPeer {
        fn boxed(
            label: &'static str,
            calls: Arc<Mutex<Vec<String>>>,
            active: Arc<AtomicUsize>,
            max_active: Arc<AtomicUsize>,
        ) -> Arc<dyn TierMutationPeer> {
            Arc::new(Self {
                label,
                calls,
                active,
                max_active,
            })
        }

        async fn track(&self, action: &str) {
            let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active.fetch_max(current, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.active.fetch_sub(1, Ordering::SeqCst);
            lock_unpoisoned(&self.calls).push(format!("{}:{action}", self.label));
        }
    }

    #[async_trait::async_trait]
    impl TierMutationPeer for ConcurrencyTrackingTierMutationPeer {
        fn peer_label(&self) -> String {
            self.label.to_string()
        }

        async fn prepare_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            self.track("prepare").await;
            Ok(PeerTierMutationState::Prepared)
        }

        async fn commit_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            self.track("commit").await;
            Ok(PeerTierMutationState::Committed)
        }

        async fn abort_tier_mutation(
            &self,
            _mutation_id: uuid::Uuid,
            _canonical_prepare_payload: Bytes,
        ) -> Result<PeerTierMutationState> {
            self.track("abort").await;
            Ok(PeerTierMutationState::Aborted)
        }
    }

    #[tokio::test]
    async fn prepare_tier_mutation_peers_uses_bounded_parallel_fanout() {
        let mutation_id = uuid::Uuid::from_u128(34);
        let mut intent = prepared_remove_intent("COLD-A", mutation_id);
        intent.expires_at_unix_nanos = tier_mutation_intent_expiry_unix_nanos();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));

        let prepared = prepare_tier_mutation_peers(
            mutation_id,
            vec![
                ConcurrencyTrackingTierMutationPeer::boxed("peer-a", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-b", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-c", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-d", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-e", calls.clone(), active.clone(), max_active.clone()),
            ],
            &intent,
        )
        .await
        .unwrap_or_else(|failure| panic!("successful prepare fanout should prepare every peer: {}", failure.error.message));

        assert_eq!(prepared.len(), 5);
        assert_eq!(
            max_active.load(Ordering::SeqCst),
            TIER_MUTATION_PEER_FANOUT_CONCURRENCY,
            "peer prepare fanout should run independently up to its concurrency bound"
        );
        let mut calls = lock_unpoisoned(&calls).clone();
        calls.sort();
        assert_eq!(
            calls,
            vec![
                "peer-a:prepare".to_string(),
                "peer-b:prepare".to_string(),
                "peer-c:prepare".to_string(),
                "peer-d:prepare".to_string(),
                "peer-e:prepare".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn prepare_fanout_refills_a_slot_while_an_earlier_peer_is_slow() {
        let mutation_id = uuid::Uuid::from_u128(0x38);
        let mut intent = prepared_remove_intent("COLD-A", mutation_id);
        intent.expires_at_unix_nanos = tier_mutation_intent_expiry_unix_nanos();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let slow_started = Arc::new(Notify::new());
        let slow_release = Arc::new(tokio::sync::Semaphore::new(0));
        let slow: Arc<dyn TierMutationPeer> = Arc::new(BlockingPrepareTierMutationPeer {
            started: slow_started.clone(),
            release: slow_release.clone(),
            calls: calls.clone(),
        });
        let peers = vec![
            slow,
            FakeTierMutationPeer::boxed("peer-b", calls.clone(), Ok(PeerTierMutationState::Prepared)),
            FakeTierMutationPeer::boxed("peer-c", calls.clone(), Ok(PeerTierMutationState::Prepared)),
            FakeTierMutationPeer::boxed("peer-d", calls.clone(), Ok(PeerTierMutationState::Prepared)),
            FakeTierMutationPeer::boxed("peer-e", calls.clone(), Ok(PeerTierMutationState::Prepared)),
        ];
        let prepare = tokio::spawn(async move { prepare_tier_mutation_peers(mutation_id, peers, &intent).await });
        slow_started.notified().await;

        tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                if lock_unpoisoned(&calls).iter().any(|call| call.starts_with("peer-e:prepare:")) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the fifth peer should refill a completed slot without waiting for the first peer");
        slow_release.add_permits(1);

        let prepared = prepare
            .await
            .expect("work-conserving Prepare task should join")
            .unwrap_or_else(|failure| panic!("work-conserving Prepare should succeed: {}", failure.error.message));
        assert_eq!(prepared.len(), 5);
    }

    #[tokio::test]
    async fn prepare_tier_mutation_peers_accepts_replayed_committed_peer() {
        let mutation_id = uuid::Uuid::from_u128(36);
        let mut intent = prepared_remove_intent("COLD-A", mutation_id);
        intent.expires_at_unix_nanos = tier_mutation_intent_expiry_unix_nanos();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let peers = vec![
            FakeTierMutationPeer::boxed_with_prepare_commit(
                "peer-a",
                calls.clone(),
                Ok(PeerTierMutationState::Committed),
                Ok(PeerTierMutationState::Committed),
            ),
            ConcurrencyTrackingTierMutationPeer::boxed(
                "peer-b",
                calls.clone(),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(AtomicUsize::new(0)),
            ),
        ];

        let prepared = match prepare_tier_mutation_peers(mutation_id, peers, &intent).await {
            Ok(prepared) => prepared,
            Err(_) => panic!("replayed committed peer should not fail the prepare fanout"),
        };

        assert_eq!(prepared.len(), 1);
        let calls = lock_unpoisoned(&calls);
        assert_eq!(calls.len(), 2);
        assert!(calls.iter().any(|call| call.starts_with("peer-a:prepare:")));
        assert!(calls.iter().any(|call| call == "peer-b:prepare"));
    }

    #[tokio::test(start_paused = true)]
    async fn prepare_timeout_is_aggregated_and_compensated_as_ambiguous() {
        let mutation_id = uuid::Uuid::from_u128(37);
        let mut intent = prepared_remove_intent("COLD-A", mutation_id);
        intent.expires_at_unix_nanos = tier_mutation_intent_expiry_unix_nanos();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let definitely_rejected = Arc::new(FakeTierMutationPeer {
            label: "rejected-peer",
            calls: calls.clone(),
            captured_prepares: None,
            prepare: Err("definitely rejected"),
            prepare_definitely_rejected: true,
            commit: Ok(PeerTierMutationState::Committed),
            abort: Ok(PeerTierMutationState::Aborted),
        }) as Arc<dyn TierMutationPeer>;
        let hanging = Arc::new(HangingPrepareTierMutationPeer { calls: calls.clone() }) as Arc<dyn TierMutationPeer>;

        let failure = match prepare_tier_mutation_peers(mutation_id, vec![hanging, definitely_rejected], &intent).await {
            Ok(_) => panic!("a timed-out and rejected prepare fanout must fail"),
            Err(failure) => failure,
        };

        assert!(failure.error.message.contains("hanging-peer: request timed out"));
        assert!(failure.error.message.contains("rejected-peer"));
        assert_eq!(failure.prepared_peers.len(), 1, "only the ambiguous timeout should require abort");
        abort_tier_mutation_peers(&intent, failure.prepared_peers)
            .await
            .expect("the ambiguous prepare timeout should receive a compensating abort");
        let calls = lock_unpoisoned(&calls);
        assert!(calls.iter().any(|call| call == "hanging-peer:prepare"));
        assert!(calls.iter().any(|call| call == "hanging-peer:abort"));
        assert!(calls.iter().any(|call| call.starts_with("rejected-peer:prepare:")));
    }

    #[tokio::test(start_paused = true)]
    async fn prepare_fanout_wide_deadline_does_not_outlive_intent_expiry() {
        let mutation_id = uuid::Uuid::from_u128(0x39);
        let mut intent = prepared_remove_intent("COLD-A", mutation_id);
        let now = OffsetDateTime::now_utc().unix_timestamp_nanos();
        intent.expires_at_unix_nanos = i64::try_from(now.saturating_add(1_000_000_000)).unwrap_or(i64::MAX);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let peers = (0..5)
            .map(|_| Arc::new(HangingPrepareTierMutationPeer { calls: calls.clone() }) as Arc<dyn TierMutationPeer>)
            .collect();
        let started_at = Instant::now();

        let failure = match prepare_tier_mutation_peers(mutation_id, peers, &intent).await {
            Ok(_) => panic!("fanout that reaches intent expiry must fail closed"),
            Err(failure) => failure,
        };

        assert!(Instant::now().duration_since(started_at) <= Duration::from_secs(1));
        assert_eq!(failure.prepared_peers.len(), TIER_MUTATION_PEER_FANOUT_CONCURRENCY);
        assert!(failure.error.message.contains("fanout deadline expired before request start"));
        assert_eq!(
            lock_unpoisoned(&calls)
                .iter()
                .filter(|call| call.as_str() == "hanging-peer:prepare")
                .count(),
            TIER_MUTATION_PEER_FANOUT_CONCURRENCY
        );
    }

    #[tokio::test]
    async fn commit_tier_mutation_peers_uses_bounded_parallel_fanout() {
        let mutation_id = uuid::Uuid::from_u128(35);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));

        commit_tier_mutation_peers(
            mutation_id,
            vec![
                ConcurrencyTrackingTierMutationPeer::boxed("peer-a", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-b", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-c", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-d", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-e", calls.clone(), active.clone(), max_active.clone()),
            ],
            "etag-new",
        )
        .await
        .expect("successful commit fanout should commit every peer");

        assert_eq!(
            max_active.load(Ordering::SeqCst),
            TIER_MUTATION_PEER_FANOUT_CONCURRENCY,
            "peer commit fanout should run independently up to its concurrency bound"
        );
        let mut calls = lock_unpoisoned(&calls).clone();
        calls.sort();
        assert_eq!(
            calls,
            vec![
                "peer-a:commit".to_string(),
                "peer-b:commit".to_string(),
                "peer-c:commit".to_string(),
                "peer-d:commit".to_string(),
                "peer-e:commit".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn commit_tier_mutation_peers_collects_every_failure() {
        let mutation_id = uuid::Uuid::from_u128(38);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let peers = [("peer-a", "commit-a-failed"), ("peer-b", "commit-b-failed")]
            .into_iter()
            .map(|(label, message)| {
                Arc::new(FakeTierMutationPeer {
                    label,
                    calls: calls.clone(),
                    captured_prepares: None,
                    prepare: Ok(PeerTierMutationState::Prepared),
                    prepare_definitely_rejected: false,
                    commit: Err(message),
                    abort: Ok(PeerTierMutationState::Aborted),
                }) as Arc<dyn TierMutationPeer>
            })
            .collect();

        let err = commit_tier_mutation_peers(mutation_id, peers, "etag-new")
            .await
            .expect_err("all peer commit failures must be reported");

        let rendered = err.to_string();
        assert!(rendered.contains("commit-a-failed"), "{rendered}");
        assert!(rendered.contains("commit-b-failed"), "{rendered}");
        assert_eq!(lock_unpoisoned(&calls).len(), 2);
    }

    #[tokio::test]
    async fn abort_tier_mutation_peers_uses_bounded_parallel_fanout() {
        let mutation_id = uuid::Uuid::from_u128(36);
        let intent = prepared_remove_intent("COLD-A", mutation_id);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let active = Arc::new(AtomicUsize::new(0));
        let max_active = Arc::new(AtomicUsize::new(0));

        abort_tier_mutation_peers(
            &intent,
            vec![
                ConcurrencyTrackingTierMutationPeer::boxed("peer-a", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-b", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-c", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-d", calls.clone(), active.clone(), max_active.clone()),
                ConcurrencyTrackingTierMutationPeer::boxed("peer-e", calls.clone(), active.clone(), max_active.clone()),
            ],
        )
        .await
        .expect("successful abort fanout should abort every peer");

        assert_eq!(
            max_active.load(Ordering::SeqCst),
            TIER_MUTATION_PEER_FANOUT_CONCURRENCY,
            "peer abort fanout should run independently up to its concurrency bound"
        );
        let mut calls = lock_unpoisoned(&calls).clone();
        calls.sort();
        assert_eq!(
            calls,
            vec![
                "peer-a:abort".to_string(),
                "peer-b:abort".to_string(),
                "peer-c:abort".to_string(),
                "peer-d:abort".to_string(),
                "peer-e:abort".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn abort_tier_mutation_peers_collects_every_failure() {
        let mutation_id = uuid::Uuid::from_u128(39);
        let intent = prepared_remove_intent("COLD-A", mutation_id);
        let calls = Arc::new(Mutex::new(Vec::new()));
        let peers = [("peer-a", "abort-a-failed"), ("peer-b", "abort-b-failed")]
            .into_iter()
            .map(|(label, message)| {
                Arc::new(FakeTierMutationPeer {
                    label,
                    calls: calls.clone(),
                    captured_prepares: None,
                    prepare: Ok(PeerTierMutationState::Prepared),
                    prepare_definitely_rejected: false,
                    commit: Ok(PeerTierMutationState::Committed),
                    abort: Err(message),
                }) as Arc<dyn TierMutationPeer>
            })
            .collect();

        let err = abort_tier_mutation_peers(&intent, peers)
            .await
            .expect_err("all peer abort failures must be reported");

        let rendered = err.to_string();
        assert!(rendered.contains("abort-a-failed"), "{rendered}");
        assert!(rendered.contains("abort-b-failed"), "{rendered}");
        assert_eq!(lock_unpoisoned(&calls).len(), 2);
    }

    #[tokio::test]
    async fn committed_mutation_recovery_replays_peer_commit() {
        let mutation_id = uuid::Uuid::from_u128(13);
        let intent = committed_remove_intent("COLD-A", mutation_id, "etag-new");
        let calls = Arc::new(Mutex::new(Vec::new()));

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::replay_committed_mutation_intents(&[intent])
                        .await
                        .expect("committed recovery should replay peer commit");
                },
            )
            .await;

        assert_eq!(lock_unpoisoned(&calls).as_slice(), &[format!("peer-a:commit:{mutation_id}:etag-new")]);
    }

    #[tokio::test]
    async fn committed_mutation_recovery_fails_closed_on_peer_commit_error() {
        let mutation_id = uuid::Uuid::from_u128(14);
        let intent = committed_remove_intent("COLD-A", mutation_id, "etag-new");
        let calls = Arc::new(Mutex::new(Vec::new()));

        let err = TIER_MUTATION_TEST_PEERS
            .scope(vec![FakeTierMutationPeer::boxed("peer-a", calls.clone(), Err("network down"))], async {
                TierConfigMgr::replay_committed_mutation_intents(&[intent]).await
            })
            .await
            .expect_err("committed recovery must fail closed when peer commit cannot replay");

        assert!(err.to_string().contains("network down"), "{err}");
        assert_eq!(lock_unpoisoned(&calls).as_slice(), &[format!("peer-a:commit:{mutation_id}:etag-new")]);
    }

    #[tokio::test]
    async fn committed_replay_mixed_version_peer_matrix_fails_before_local_publish() {
        for (case, peer_error, expected_error) in [
            (
                "old_unimplemented",
                "peer tier mutation commit RPC failed: status: Unimplemented, message: \"old peer has no tier mutation control service\"",
                "old peer",
            ),
            (
                "deadline_exceeded",
                "peer tier mutation commit RPC failed: status: DeadlineExceeded, message: \"peer tier mutation control timed out\"",
                "timed out",
            ),
            (
                "unavailable",
                "peer tier mutation commit RPC failed: status: Unavailable, message: \"peer tier mutation control unavailable\"",
                "unavailable",
            ),
        ] {
            let store = Arc::new(CasConfigStore::default());
            let mut persisted = empty_mgr();
            persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
            persisted
                .save_tiering_config_if_current(store.clone(), None)
                .await
                .expect("tier config fixture should persist");

            let mutation_id = uuid::Uuid::from_u128(match case {
                "old_unimplemented" => 31,
                "deadline_exceeded" => 32,
                "unavailable" => 33,
                _ => unreachable!("matrix cases are enumerated above"),
            });
            let intent = committed_remove_intent("COLD-A", mutation_id, "etag-new");
            crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &intent)
                .await
                .expect("committed intent fixture should persist");
            let writes_after_fixture = store.next_etag.load(Ordering::SeqCst);
            let calls = Arc::new(Mutex::new(Vec::new()));
            let handle = TierConfigMgr::new();

            let err = TIER_MUTATION_TEST_PEERS
                .scope(vec![FakeTierMutationPeer::boxed("peer-a", calls.clone(), Err(peer_error))], async {
                    TierConfigMgr::reload_handle_with(&handle, store.clone()).await
                })
                .await
                .expect_err("mixed-version peer failure must abort committed replay");

            assert!(err.to_string().contains(expected_error), "{case} returned {err}");
            assert_eq!(lock_unpoisoned(&calls).as_slice(), &[format!("peer-a:commit:{mutation_id}:etag-new")]);
            assert!(
                !handle.read().await.tiers.contains_key("COLD-A"),
                "{case} must not publish the local tier config after peer replay fails"
            );
            assert_eq!(
                store.next_etag.load(Ordering::SeqCst),
                writes_after_fixture,
                "{case} must not perform a second tier-config CAS write after replay fails"
            );
            let reloaded = TierConfigMgr::load_tier_mutation_intents(store)
                .await
                .expect("intent scan should succeed after failed mixed-version replay");
            assert!(
                reloaded.iter().any(|intent| intent.mutation_id == mutation_id),
                "{case} must keep the committed intent for retry"
            );
            let blocked = match TierConfigMgr::acquire_operation_lease(&handle, "COLD-A").await {
                Ok(_) => panic!("{case} must keep committed replay blocking old tier operations"),
                Err(err) => err,
            };
            assert!(blocked.message.contains("being replaced"), "{case} returned {blocked}");
        }
    }

    #[tokio::test]
    async fn intent_advance_treats_matching_committed_cas_race_as_idempotent() {
        let store = Arc::new(CasConfigStore::default());
        let mutation_id = uuid::Uuid::from_u128(34);
        let prepared = prepared_remove_intent("COLD-A", mutation_id);
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &prepared)
            .await
            .expect("prepared intent fixture should persist");

        let committed = committed_remove_intent("COLD-A", mutation_id, "etag-new");
        let object = crate::services::tier::tier_mutation_intent::tier_mutation_intent_record_object_name(mutation_id)
            .expect("record object should build");
        store
            .rewrite_on_next_if_match(object, committed.encode().expect("committed intent fixture should encode"))
            .await;

        let (observed, applied) = crate::services::tier::tier_mutation_intent::advance_tier_mutation_intent_record_idempotent(
            store,
            mutation_id,
            TierMutationIntentState::Committed,
            Some("etag-new".to_string()),
        )
        .await
        .expect("same-state CAS race should be idempotent");

        assert!(!applied);
        assert_eq!(observed.state, TierMutationIntentState::Committed);
        assert_eq!(observed.committed_config_etag.as_deref(), Some("etag-new"));
    }

    #[tokio::test]
    async fn intent_advance_reconciles_matching_commit_after_final_cas_race() {
        let store = Arc::new(CasConfigStore::default());
        let mutation_id = uuid::Uuid::from_u128(37);
        let prepared = prepared_remove_intent("COLD-A", mutation_id);
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &prepared)
            .await
            .expect("prepared intent fixture should persist");

        let committed = committed_remove_intent("COLD-A", mutation_id, "etag-new");
        let object = crate::services::tier::tier_mutation_intent::tier_mutation_intent_record_object_name(mutation_id)
            .expect("record object should build");
        store
            .rewrite_on_next_if_match(object.clone(), prepared.encode().expect("prepared intent fixture should encode"))
            .await;
        store
            .rewrite_on_next_if_match(object.clone(), prepared.encode().expect("prepared intent fixture should encode"))
            .await;
        store
            .rewrite_on_next_if_match(object, committed.encode().expect("committed intent fixture should encode"))
            .await;

        let (observed, applied) = crate::services::tier::tier_mutation_intent::advance_tier_mutation_intent_record_idempotent(
            store,
            mutation_id,
            TierMutationIntentState::Committed,
            Some("etag-new".to_string()),
        )
        .await
        .expect("matching commit after the final CAS race should be idempotent");

        assert!(!applied);
        assert_eq!(observed.state, TierMutationIntentState::Committed);
        assert_eq!(observed.committed_config_etag.as_deref(), Some("etag-new"));
    }

    #[tokio::test]
    async fn intent_advance_rejects_unresolved_final_cas_race() {
        let store = Arc::new(CasConfigStore::default());
        let mutation_id = uuid::Uuid::from_u128(38);
        let prepared = prepared_remove_intent("COLD-A", mutation_id);
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &prepared)
            .await
            .expect("prepared intent fixture should persist");

        let object = crate::services::tier::tier_mutation_intent::tier_mutation_intent_record_object_name(mutation_id)
            .expect("record object should build");
        const CAS_ATTEMPTS_UNDER_TEST: usize = 3;
        for _ in 0..CAS_ATTEMPTS_UNDER_TEST {
            store
                .rewrite_on_next_if_match(object.clone(), prepared.encode().expect("prepared intent fixture should encode"))
                .await;
        }

        let err = crate::services::tier::tier_mutation_intent::advance_tier_mutation_intent_record_idempotent(
            store,
            mutation_id,
            TierMutationIntentState::Committed,
            Some("etag-new".to_string()),
        )
        .await
        .expect_err("a still-prepared intent after the final CAS race must fail closed");

        assert!(matches!(err, Error::PreconditionFailed));
    }

    #[tokio::test]
    async fn committed_mutation_recovery_requires_every_peer_to_commit() {
        let mutation_id = uuid::Uuid::from_u128(17);
        let intent = committed_remove_intent("COLD-A", mutation_id, "etag-new");
        let calls = Arc::new(Mutex::new(Vec::new()));

        let err = TIER_MUTATION_TEST_PEERS
            .scope(
                vec![
                    FakeTierMutationPeer::boxed("peer-a", calls.clone(), Ok(PeerTierMutationState::Committed)),
                    FakeTierMutationPeer::boxed("peer-b", calls.clone(), Ok(PeerTierMutationState::Prepared)),
                ],
                async { TierConfigMgr::replay_committed_mutation_intents(&[intent]).await },
            )
            .await
            .expect_err("committed recovery must fail unless every peer reports committed");

        assert!(err.to_string().contains("unexpected state"), "{err}");
        assert_eq!(
            lock_unpoisoned(&calls).as_slice(),
            &[
                format!("peer-a:commit:{mutation_id}:etag-new"),
                format!("peer-b:commit:{mutation_id}:etag-new")
            ]
        );
    }

    #[test]
    fn committed_replay_rejects_partial_peer_discovery() {
        ensure_complete_tier_mutation_commit_peer_set(0, 0).expect("local-only topology has no remote peers");
        ensure_complete_tier_mutation_commit_peer_set(2, 2).expect("two remote hosts should require two peers");
        let err = ensure_complete_tier_mutation_commit_peer_set(1, 2).expect_err("missing one expected peer must fail closed");
        assert!(err.to_string().contains("without peer commit clients"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn tier_mutation_peer_composition_preserves_unresolved_topology_slots() {
        let mut endpoints = Vec::new();
        for disk_index in 0..4 {
            let mut endpoint = Endpoint::try_from(format!("http://rustfs-{disk_index}.invalid:9000/data{disk_index}").as_str())
                .expect("unresolved topology endpoint should parse without DNS");
            endpoint.is_local = disk_index == 0;
            endpoint.set_pool_index(0);
            endpoint.set_set_index(0);
            endpoint.set_disk_index(disk_index);
            endpoints.push(endpoint);
        }
        let topology = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 4,
            endpoints: Endpoints::from(endpoints),
            cmd_line: "unresolved-tier-mutation-topology".to_string(),
            platform: "test".to_string(),
        }]);
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;

        let peers = remote_tier_mutation_peers_from_topology(topology)
            .await
            .expect("every unresolved remote topology slot should retain a tier mutation client");
        assert_eq!(
            peers.iter().map(|peer| peer.peer_label()).collect::<Vec<_>>(),
            vec![
                "http://rustfs-1.invalid:9000".to_string(),
                "http://rustfs-2.invalid:9000".to_string(),
                "http://rustfs-3.invalid:9000".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn coordinator_fanout_prepare_failure_aborts_prepared_peers_without_cas() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (mut candidate, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should reload");
        let original_version = version.clone();
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("candidate")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;
        let calls = Arc::new(Mutex::new(Vec::new()));

        let err = TIER_MUTATION_TEST_PEERS
            .scope(
                vec![
                    FakeTierMutationPeer::boxed_with_prepare_commit(
                        "peer-a",
                        calls.clone(),
                        Ok(PeerTierMutationState::Prepared),
                        Ok(PeerTierMutationState::Committed),
                    ),
                    FakeTierMutationPeer::boxed_with_prepare_commit(
                        "peer-b",
                        calls.clone(),
                        Err("unsupported tier mutation peer protocol version: 4"),
                        Ok(PeerTierMutationState::Committed),
                    ),
                ],
                async {
                    TierConfigMgr::update_candidate_owned(
                        &manager,
                        store.clone(),
                        candidate,
                        version,
                        TierCandidateMutation::Remove("COLD-A".to_string(), true),
                        update,
                        None,
                    )
                    .await
                },
            )
            .await
            .expect_err("partial prepare failure must fail before config CAS");

        let TierConfigUpdateError::Publish(err) = err else {
            panic!("prepare fanout failure should be reported as publish failure");
        };
        assert!(err.message.contains("peer-b"), "{}", err.message);
        let calls = lock_unpoisoned(&calls).clone();
        assert!(calls.iter().any(|call| call.starts_with("peer-a:prepare:")), "{calls:?}");
        assert!(calls.iter().any(|call| call.starts_with("peer-b:prepare:")), "{calls:?}");
        assert!(calls.iter().any(|call| call.starts_with("peer-a:abort:")), "{calls:?}");
        assert!(
            calls.iter().any(|call| call.starts_with("peer-b:abort:")),
            "plain error text must remain ambiguous and receive Abort: {calls:?}"
        );
        assert!(!calls.iter().any(|call| call.contains(":commit:")), "{calls:?}");
        let (persisted, current_version) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config should remain readable");
        assert_eq!(current_version, original_version, "prepare failure must not CAS the tier config object");
        assert!(persisted.tiers.contains_key("COLD-A"));
        let intents = TierConfigMgr::load_coordinator_mutation_intents(store)
            .await
            .expect("coordinator intent scan should succeed");
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].state, TierMutationIntentState::Aborted);
    }

    #[tokio::test]
    async fn coordinator_old_protocol_rejection_does_not_leave_local_prepared_block() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (mut candidate, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should reload");
        let original_version = version.clone();
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("candidate")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let old_peer = Arc::new(FakeTierMutationPeer {
            label: "peer-v3",
            calls: calls.clone(),
            captured_prepares: None,
            prepare: Err("unsupported tier mutation peer protocol version: 4"),
            prepare_definitely_rejected: true,
            commit: Ok(PeerTierMutationState::Committed),
            abort: Err("unsupported tier mutation peer protocol version: 4"),
        }) as Arc<dyn TierMutationPeer>;

        let err = TIER_MUTATION_TEST_PEERS
            .scope(vec![old_peer], async {
                TierConfigMgr::update_candidate_owned(
                    &manager,
                    store.clone(),
                    candidate,
                    version,
                    TierCandidateMutation::Remove("COLD-A".to_string(), true),
                    update,
                    None,
                )
                .await
            })
            .await
            .expect_err("an old v3 peer must reject the v4 mutation before config CAS");
        assert!(matches!(err, TierConfigUpdateError::Publish(_)));
        let calls = lock_unpoisoned(&calls).clone();
        assert_eq!(calls.len(), 1, "an explicit pre-dispatch rejection must not receive Abort: {calls:?}");
        assert!(calls[0].starts_with("peer-v3:prepare:"), "{calls:?}");

        let (unchanged, current_version) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config should remain readable");
        assert_eq!(current_version, original_version);
        assert!(unchanged.tiers.contains_key("COLD-A"));
        let intents = TierConfigMgr::load_coordinator_mutation_intents(store)
            .await
            .expect("coordinator intent should be readable");
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].state, TierMutationIntentState::Aborted);
        TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old-peer rejection must restore the local data path immediately");
    }

    #[tokio::test]
    async fn coordinator_prepare_abort_failure_retains_intent_until_reload_converges() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (mut candidate, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should reload");
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("candidate")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;
        let calls = Arc::new(Mutex::new(Vec::new()));

        let err = TIER_MUTATION_TEST_PEERS
            .scope(
                vec![
                    Arc::new(FakeTierMutationPeer {
                        label: "peer-a",
                        calls: calls.clone(),
                        captured_prepares: None,
                        prepare: Ok(PeerTierMutationState::Prepared),
                        prepare_definitely_rejected: false,
                        commit: Ok(PeerTierMutationState::Committed),
                        abort: Err("abort unavailable"),
                    }) as Arc<dyn TierMutationPeer>,
                    FakeTierMutationPeer::boxed_with_prepare_commit(
                        "peer-b",
                        calls.clone(),
                        Err("prepare unavailable"),
                        Ok(PeerTierMutationState::Committed),
                    ),
                ],
                async {
                    TierConfigMgr::update_candidate_owned(
                        &manager,
                        store.clone(),
                        candidate,
                        version,
                        TierCandidateMutation::Remove("COLD-A".to_string(), true),
                        update,
                        None,
                    )
                    .await
                },
            )
            .await
            .expect_err("failed abort must retain durable coordinator recovery evidence");
        assert!(matches!(err, TierConfigUpdateError::Publish(_)));

        let intents = TierConfigMgr::load_coordinator_mutation_intents(store.clone())
            .await
            .expect("coordinator intent scan should retain abort recovery evidence");
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].state, TierMutationIntentState::Prepared);
        let prepared_intent = intents[0].clone();
        let mutation_id = intents[0].mutation_id;
        TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("ambiguous peer cleanup must not leave the coordinator data path fenced");

        let calls_before_unexpired_reload = lock_unpoisoned(&calls).clone();

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![
                    FakeTierMutationPeer::boxed("peer-a", calls.clone(), Ok(PeerTierMutationState::Committed)),
                    FakeTierMutationPeer::boxed("peer-b", calls.clone(), Ok(PeerTierMutationState::Committed)),
                ],
                async {
                    TierConfigMgr::reload_handle_with(&manager, store.clone())
                        .await
                        .expect("reload should retain an unexpired prepared coordinator intent");
                },
            )
            .await;
        assert_eq!(
            *lock_unpoisoned(&calls),
            calls_before_unexpired_reload,
            "recovery must not abort an unexpired prepared owner"
        );
        let intents = TierConfigMgr::load_coordinator_mutation_intents(store.clone())
            .await
            .expect("unexpired coordinator intent should remain readable");
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].state, TierMutationIntentState::Prepared);

        store.expire_coordinator_intent(prepared_intent).await;
        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![
                    FakeTierMutationPeer::boxed("peer-a", calls.clone(), Ok(PeerTierMutationState::Committed)),
                    FakeTierMutationPeer::boxed("peer-b", calls.clone(), Ok(PeerTierMutationState::Committed)),
                ],
                async {
                    TierConfigMgr::reload_handle_with(&manager, store.clone())
                        .await
                        .expect("reload should retry the expired durable coordinator abort");
                },
            )
            .await;

        let calls = lock_unpoisoned(&calls).clone();
        assert!(
            calls
                .iter()
                .filter(|call| *call == &format!("peer-a:abort:{mutation_id}"))
                .count()
                >= 2,
            "{calls:?}"
        );
        assert!(calls.iter().any(|call| call == &format!("peer-b:abort:{mutation_id}")), "{calls:?}");
        let intents = TierConfigMgr::load_coordinator_mutation_intents(store)
            .await
            .expect("coordinator intent scan should succeed after recovery");
        assert!(intents.is_empty(), "successful abort recovery should remove the coordinator intent");
    }

    #[tokio::test]
    async fn coordinator_fanout_commit_failure_keeps_committed_intent_for_reload_retry() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (mut candidate, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should reload");
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("candidate")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;
        let calls = Arc::new(Mutex::new(Vec::new()));

        let err = TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed_with_prepare_commit(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Prepared),
                    Err("commit unavailable"),
                )],
                async {
                    TierConfigMgr::update_candidate_owned(
                        &manager,
                        store.clone(),
                        candidate,
                        version,
                        TierCandidateMutation::Remove("COLD-A".to_string(), true),
                        update,
                        None,
                    )
                    .await
                },
            )
            .await
            .expect_err("commit peer failure must fail closed before local publish");

        let TierConfigUpdateError::Publish(err) = err else {
            panic!("commit fanout failure should be reported as publish failure");
        };
        assert!(err.message.contains("commit unavailable"), "{}", err.message);
        assert!(
            manager.read().await.tiers.contains_key("COLD-A"),
            "failed peer commit must not publish the new generation locally"
        );
        let blocked = match TierConfigMgr::acquire_operation_lease(&manager, "COLD-A").await {
            Ok(_) => panic!("a committed coordinator mutation must fence the old local generation"),
            Err(err) => err,
        };
        assert!(
            blocked.message.contains(TIER_MUTATION_BLOCK_MESSAGE),
            "committed coordinator mutation should retain the local fence: {blocked}"
        );
        let intents = TierConfigMgr::load_coordinator_mutation_intents(store.clone())
            .await
            .expect("coordinator intent scan should succeed after commit failure");
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].state, TierMutationIntentState::Committed);
        let committed_id = intents[0].mutation_id;
        let committed_etag = intents[0]
            .committed_config_etag
            .clone()
            .expect("committed coordinator intent should carry the saved tier config ETag");

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::reload_handle_with(&manager, store.clone())
                        .await
                        .expect("reload should retry committed coordinator peer fanout and publish");
                },
            )
            .await;

        assert!(
            lock_unpoisoned(&calls)
                .iter()
                .any(|call| call == &format!("peer-a:commit:{committed_id}:{committed_etag}")),
            "reload should retry the committed coordinator peer commit: {:?}",
            lock_unpoisoned(&calls)
        );
        assert!(
            !manager.read().await.tiers.contains_key("COLD-A"),
            "reload retry should publish the committed tier removal"
        );
        let intents = TierConfigMgr::load_coordinator_mutation_intents(store)
            .await
            .expect("coordinator intent scan should succeed after retry cleanup");
        assert!(intents.iter().all(|intent| intent.mutation_id != committed_id));
    }

    #[tokio::test]
    async fn coordinator_intent_commit_failure_keeps_local_fence_for_recovery() {
        use crate::services::tier::tier_mutation_intent::TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX;

        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("coordinator CAS failure fixture should persist");
        let (mut candidate, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("coordinator CAS failure fixture should reload");
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("candidate")));
        store
            .fail_put_with_prefix_after(TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX, 1, false)
            .await;
        let update = TierConfigMgr::admin_update_lock(&manager).await;

        let err = TIER_MUTATION_TEST_PEERS
            .scope(Vec::new(), async {
                TierConfigMgr::update_candidate_owned(
                    &manager,
                    store.clone(),
                    candidate,
                    version,
                    TierCandidateMutation::Remove("COLD-A".to_string(), true),
                    update,
                    None,
                )
                .await
            })
            .await
            .expect_err("coordinator committed-state CAS failure must be observable");
        assert!(matches!(err, TierConfigUpdateError::Save(_)));
        assert!(manager.read().await.tiers.contains_key("COLD-A"));
        assert!(TierConfigMgr::has_committed_mutation_block(&manager).await);
        let refresh = TierConfigMgr::mutation_refresh_notifier(&manager).await;
        tokio::time::timeout(Duration::from_secs(1), refresh.notified())
            .await
            .expect("failed coordinator commit must notify recovery after saving config");
        let blocked = match TierConfigMgr::acquire_operation_lease(&manager, "COLD-A").await {
            Ok(_) => panic!("failed coordinator commit CAS must retain the local committed fence"),
            Err(err) => err,
        };
        assert!(blocked.message.contains(TIER_MUTATION_BLOCK_MESSAGE), "{blocked}");
        let intents = TierConfigMgr::load_coordinator_mutation_intents(store.clone())
            .await
            .expect("prepared coordinator proof should remain readable");
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].state, TierMutationIntentState::Prepared);
        assert!(
            load_tier_config_for_update(store.clone())
                .await
                .expect("committed config should remain readable")
                .0
                .tiers
                .is_empty(),
            "the tier config save must precede the injected coordinator CAS failure"
        );

        TIER_MUTATION_TEST_PEERS
            .scope(Vec::new(), async {
                TierConfigMgr::reload_handle_with(&manager, store.clone())
                    .await
                    .expect("reload should finish the committed coordinator intent");
            })
            .await;
        assert!(!manager.read().await.tiers.contains_key("COLD-A"));
        assert!(
            TierConfigMgr::load_coordinator_mutation_intents(store)
                .await
                .expect("coordinator intent cleanup scan should succeed")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn post_commit_config_put_error_is_reconciled_by_read_back() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("post-commit error fixture should persist");
        let (mut candidate, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("post-commit error fixture should reload");
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("candidate")));
        store
            .fail_put_with_prefix_after(&tier_config_path(TIER_CONFIG_FILE), 0, true)
            .await;
        let update = TierConfigMgr::admin_update_lock(&manager).await;

        TIER_MUTATION_TEST_PEERS
            .scope(Vec::new(), async {
                TierConfigMgr::update_candidate_owned(
                    &manager,
                    store.clone(),
                    candidate,
                    version,
                    TierCandidateMutation::Remove("COLD-A".to_string(), true),
                    update,
                    None,
                )
                .await
                .expect("read-back should recognize a committed config after the PUT response is lost");
            })
            .await;

        assert!(!manager.read().await.tiers.contains_key("COLD-A"));
        assert!(
            load_tier_config_for_update(store.clone())
                .await
                .expect("post-commit config should remain readable")
                .0
                .tiers
                .is_empty()
        );
        TIER_MUTATION_TEST_PEERS
            .scope(Vec::new(), async {
                TierConfigMgr::reload_handle_with(&manager, store.clone())
                    .await
                    .expect("reload should clean the committed coordinator proof");
            })
            .await;
        assert!(
            TierConfigMgr::load_coordinator_mutation_intents(store)
                .await
                .expect("coordinator cleanup scan should succeed")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn coordinator_fanout_success_commits_peers_before_publish() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (mut candidate, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should reload");
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("candidate")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;
        let calls = Arc::new(Mutex::new(Vec::new()));

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::update_candidate_owned(
                        &manager,
                        store.clone(),
                        candidate,
                        version,
                        TierCandidateMutation::Remove("COLD-A".to_string(), true),
                        update,
                        None,
                    )
                    .await
                    .expect("successful peer fanout should publish the tier mutation");
                },
            )
            .await;

        let calls = lock_unpoisoned(&calls).clone();
        let prepare_index = calls
            .iter()
            .position(|call| call.starts_with("peer-a:prepare:"))
            .expect("successful update should prepare the peer");
        let commit_index = calls
            .iter()
            .position(|call| call.contains(":commit:"))
            .expect("successful update should commit the peer");
        assert!(prepare_index < commit_index, "{calls:?}");
        let saved_etag = load_tier_config_for_update(store)
            .await
            .expect("saved tier config should reload")
            .1
            .expect("saved tier config should have an ETag");
        assert!(calls.iter().any(|call| call.ends_with(&format!(":{saved_etag}"))), "{calls:?}");
        assert!(
            !manager.read().await.tiers.contains_key("COLD-A"),
            "local manager should publish only after peer commit succeeds"
        );
        let guard = manager.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("successful mutation should retain its runtime registry");
        assert!(
            lock_unpoisoned(&runtime).committed_mutation_blocks.is_empty(),
            "successful local publish must retire its committed mutation fence"
        );
    }

    #[tokio::test]
    async fn coordinator_fanout_commits_prepared_peers_after_replayed_committed_prepare() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (mut candidate, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should reload");
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("candidate")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;
        let calls = Arc::new(Mutex::new(Vec::new()));

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![
                    FakeTierMutationPeer::boxed_with_prepare_commit(
                        "peer-a",
                        calls.clone(),
                        Ok(PeerTierMutationState::Committed),
                        Ok(PeerTierMutationState::Committed),
                    ),
                    FakeTierMutationPeer::boxed_with_prepare_commit(
                        "peer-b",
                        calls.clone(),
                        Ok(PeerTierMutationState::Prepared),
                        Ok(PeerTierMutationState::Committed),
                    ),
                ],
                async {
                    TierConfigMgr::update_candidate_owned(
                        &manager,
                        store.clone(),
                        candidate,
                        version,
                        TierCandidateMutation::Remove("COLD-A".to_string(), true),
                        update,
                        None,
                    )
                    .await
                    .expect("committed replay plus partial prepare should publish after prepared peers commit");
                },
            )
            .await;

        let calls = lock_unpoisoned(&calls).clone();
        let peer_a_prepare_call = calls
            .iter()
            .find(|call| call.starts_with("peer-a:prepare:"))
            .expect("replayed peer should still receive prepare");
        let peer_b_prepare = calls
            .iter()
            .position(|call| call.starts_with("peer-b:prepare:"))
            .expect("remaining peer should prepare the mutation");
        let peer_b_commit = calls
            .iter()
            .position(|call| call.starts_with("peer-b:commit:"))
            .expect("prepared peer should receive commit");
        assert!(peer_b_prepare < peer_b_commit, "{calls:?}");
        let peer_b_prepare_id = calls[peer_b_prepare]
            .split(':')
            .nth(2)
            .expect("prepare call should record the mutation id");
        let peer_b_commit_id = calls[peer_b_commit]
            .split(':')
            .nth(2)
            .expect("commit call should record the mutation id");
        let peer_a_prepare_id = peer_a_prepare_call
            .split(':')
            .nth(2)
            .expect("replayed prepare call should record the mutation id");
        assert_eq!(peer_b_commit_id, peer_b_prepare_id, "{calls:?}");
        assert_eq!(peer_a_prepare_id, peer_b_prepare_id, "{calls:?}");
        assert!(
            !calls.iter().any(|call| call.starts_with("peer-a:commit:")),
            "already-committed prepare replay should not receive a duplicate commit: {calls:?}"
        );
        let saved_etag = load_tier_config_for_update(store.clone())
            .await
            .expect("saved tier config should reload")
            .1
            .expect("saved tier config should have an ETag");
        assert!(
            calls
                .iter()
                .any(|call| call.starts_with("peer-b:commit:") && call.ends_with(&format!(":{saved_etag}"))),
            "prepared peer commit should carry the saved tier config ETag: {calls:?}"
        );
        assert!(
            !manager.read().await.tiers.contains_key("COLD-A"),
            "local manager should publish after the partially prepared peer commits"
        );
    }

    #[tokio::test]
    async fn coordinator_fanout_converges_four_hot_reporter_committed_prepare_replay() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        empty_mgr()
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("empty tier config fixture should persist");
        let (candidate, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should reload");
        let update = TierConfigMgr::admin_update_lock(&manager).await;
        let calls = Arc::new(Mutex::new(Vec::new()));

        TIER_DRIVER_TEST_FACTORY
            .scope(
                healthy_driver_factory(),
                TIER_MUTATION_TEST_PEERS.scope(
                    vec![
                        FakeTierMutationPeer::boxed_with_prepare_commit(
                            "peer-a",
                            calls.clone(),
                            Ok(PeerTierMutationState::Committed),
                            Ok(PeerTierMutationState::Committed),
                        ),
                        FakeTierMutationPeer::boxed_with_prepare_commit(
                            "peer-b",
                            calls.clone(),
                            Ok(PeerTierMutationState::Prepared),
                            Ok(PeerTierMutationState::Committed),
                        ),
                        FakeTierMutationPeer::boxed_with_prepare_commit(
                            "peer-c",
                            calls.clone(),
                            Ok(PeerTierMutationState::Prepared),
                            Ok(PeerTierMutationState::Committed),
                        ),
                    ],
                    async {
                        TierConfigMgr::update_candidate_owned(
                            &manager,
                            store.clone(),
                            candidate,
                            version,
                            TierCandidateMutation::Add(Box::new(build_rustfs_tier("COLD-A")), true),
                            update,
                            None,
                        )
                        .await
                        .expect("four-hot AddTier reporter replay should publish after prepared peers commit");
                    },
                ),
            )
            .await;

        let calls = lock_unpoisoned(&calls).clone();
        let reporter_prepare_call = calls
            .iter()
            .find(|call| call.starts_with("peer-a:prepare:"))
            .expect("reporter peer should receive prepare replay");
        let reporter_mutation_id = reporter_prepare_call
            .split(':')
            .nth(2)
            .expect("reporter prepare call should record the mutation id");
        assert!(
            !calls.iter().any(|call| call.starts_with("peer-a:commit:")),
            "already-committed reporter must not receive a duplicate commit: {calls:?}"
        );

        let saved_etag = load_tier_config_for_update(store)
            .await
            .expect("saved tier config should reload")
            .1
            .expect("saved tier config should have an ETag");
        for peer in ["peer-b", "peer-c"] {
            let prepare_index = calls
                .iter()
                .position(|call| call.starts_with(&format!("{peer}:prepare:")))
                .expect("prepared peer should receive prepare");
            let commit_index = calls
                .iter()
                .position(|call| call.starts_with(&format!("{peer}:commit:")))
                .expect("prepared peer should receive commit");
            assert!(prepare_index < commit_index, "{peer} commit must follow prepare: {calls:?}");
            let prepared_mutation_id = calls[prepare_index]
                .split(':')
                .nth(2)
                .expect("prepare call should record the mutation id");
            let committed_mutation_id = calls[commit_index]
                .split(':')
                .nth(2)
                .expect("commit call should record the mutation id");
            assert_eq!(
                prepared_mutation_id, reporter_mutation_id,
                "{peer} prepare should share the reporter mutation id"
            );
            assert_eq!(
                committed_mutation_id, reporter_mutation_id,
                "{peer} commit should share the reporter mutation id"
            );
            assert!(
                calls[commit_index].ends_with(&format!(":{saved_etag}")),
                "{peer} commit should carry the saved tier config ETag: {calls:?}"
            );
        }
        assert!(
            manager.read().await.tiers.contains_key("COLD-A"),
            "local manager should publish the AddTier candidate after peer convergence"
        );
    }

    #[tokio::test]
    async fn reload_handle_replays_committed_mutation_and_removes_intent_record() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");

        let mutation_id = uuid::Uuid::from_u128(15);
        let intent = committed_remove_intent("COLD-A", mutation_id, "etag-new");
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &intent)
            .await
            .expect("committed intent fixture should persist");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let handle = TierConfigMgr::new();

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::reload_handle_with(&handle, store.clone())
                        .await
                        .expect("reload should replay committed intent before publishing");
                },
            )
            .await;

        assert_eq!(lock_unpoisoned(&calls).as_slice(), &[format!("peer-a:commit:{mutation_id}:etag-new")]);
        assert!(handle.read().await.tiers.contains_key("COLD-A"));
        let reloaded = TierConfigMgr::load_tier_mutation_intents(store)
            .await
            .expect("intent scan should succeed after cleanup");
        assert!(
            reloaded.iter().all(|intent| intent.mutation_id != mutation_id),
            "committed intent should be removed after successful replay"
        );
    }

    #[test]
    fn tier_mutation_tombstone_retention_includes_clock_skew_boundary() {
        let mut intent = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(0x151));
        intent.expires_at_unix_nanos = 10_000;
        intent
            .advance(TierMutationIntentState::Aborted, None)
            .expect("fixture should advance to an aborted tombstone");
        let skew = i64::try_from(TIER_MUTATION_TOMBSTONE_CLOCK_SKEW.as_nanos()).expect("clock skew should fit i64");
        assert!(!TierConfigMgr::tier_mutation_tombstone_retention_elapsed(
            &intent,
            intent.expires_at_unix_nanos.saturating_add(skew).saturating_sub(1),
        ));
        assert!(TierConfigMgr::tier_mutation_tombstone_retention_elapsed(
            &intent,
            intent.expires_at_unix_nanos.saturating_add(skew),
        ));
    }

    #[tokio::test]
    async fn published_peer_terminal_tombstone_is_retained_without_runtime_fence() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (_, current_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should load with metadata");
        let current_etag = current_etag.expect("tier config fixture should have an ETag");
        let handle = TierConfigMgr::new();
        TierConfigMgr::reload_handle_with(&handle, store.clone())
            .await
            .expect("initial reload should publish the authoritative config");

        let mutation_id = uuid::Uuid::from_u128(0x152);
        let mut intent = committed_remove_intent("COLD-A", mutation_id, &current_etag);
        intent.candidate_digest = tier_config_candidate_digest(&persisted).expect("fixture digest should build");
        intent.expires_at_unix_nanos = i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos())
            .unwrap_or(i64::MAX)
            .saturating_add(i64::try_from(TIER_MUTATION_INTENT_TTL.as_nanos()).expect("intent TTL should fit i64"));
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &intent)
            .await
            .expect("fresh terminal fixture should persist");
        let calls = Arc::new(Mutex::new(Vec::new()));

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::reload_handle_with(&handle, store.clone())
                        .await
                        .expect("published terminal tombstone should reload");
                    TierConfigMgr::reload_handle_with(&handle, store.clone())
                        .await
                        .expect("retained tombstone reload should be idempotent");
                },
            )
            .await;

        assert!(lock_unpoisoned(&calls).is_empty(), "a settled tombstone must not replay peer Commit");
        let retained = TierConfigMgr::load_tier_mutation_intents(store)
            .await
            .expect("retained tombstone should remain readable");
        assert_eq!(retained, vec![intent]);
        let guard = handle.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
        assert!(
            lock_unpoisoned(&runtime).committed_mutation_blocks.is_empty(),
            "a retained settled tombstone must not block the published runtime"
        );
    }

    #[tokio::test]
    async fn published_dual_terminal_intent_does_not_restore_local_runtime_fence_during_replay() {
        use crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record;

        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("published tier config fixture should persist");
        let (_, current_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("published tier config fixture should load with metadata");
        let current_etag = current_etag.expect("published tier config fixture should have an ETag");
        let affected_targets = build_tier_mutation_affected_targets(
            TierMutationIntentKind::Add,
            HashSet::from(["COLD-A".to_string()]),
            &empty_mgr(),
            &persisted,
        )
        .expect("published AddTier targets should build");
        let mut intent = build_coordinator_tier_mutation_intent(TierMutationIntentKind::Add, None, &persisted, affected_targets)
            .expect("published AddTier intent should build")
            .expect("published AddTier should require a durable intent");
        intent
            .advance(TierMutationIntentState::Committed, Some(current_etag))
            .expect("published AddTier intent should commit");
        save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &intent)
            .await
            .expect("published coordinator intent should persist");
        save_tier_mutation_intent_record(store.clone(), &intent)
            .await
            .expect("published peer intent should persist");

        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("published"));
        }
        {
            let guard = manager.read().await;
            assert_eq!(
                tier_config_candidate_digest(&guard).expect("published manager digest should build"),
                intent.candidate_digest
            );
        }
        TierConfigMgr::apply_committed_mutation_intent_block(&manager, &intent)
            .await
            .expect("pre-existing committed runtime fence should install");
        assert!(
            TierConfigMgr::acquire_operation_lease(&manager, "COLD-A").await.is_err(),
            "fixture must begin with the committed runtime fence installed"
        );

        let started = Arc::new(Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![Arc::new(BlockingCommitTierMutationPeer {
                    started: started.clone(),
                    release: release.clone(),
                })],
                async {
                    let reload = TierConfigMgr::reload_handle_with(&manager, store.clone());
                    tokio::pin!(reload);
                    tokio::time::timeout(Duration::from_secs(5), async {
                        tokio::select! {
                            result = &mut reload => panic!("reload finished before terminal replay was released: {result:?}"),
                            _ = started.notified() => {}
                        }
                    })
                    .await
                    .expect("terminal replay should reach the blocking peer");

                    let lease = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
                        .await
                        .expect("terminal cleanup must not re-fence an already-published tier");
                    drop(lease);
                    release.add_permits(1);
                    tokio::time::timeout(Duration::from_secs(5), &mut reload)
                        .await
                        .expect("terminal replay should finish after the peer responds")
                        .expect("terminal replay should succeed after the peer responds");
                },
            )
            .await;

        assert!(manager.read().await.tiers.contains_key("COLD-A"));
        assert!(
            TierConfigMgr::load_coordinator_mutation_intents(store.clone())
                .await
                .expect("coordinator cleanup should be readable")
                .is_empty()
        );
        assert_eq!(
            TierConfigMgr::load_tier_mutation_intents(store)
                .await
                .expect("retained peer tombstone should be readable"),
            vec![intent]
        );
        let guard = manager.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
        assert!(
            lock_unpoisoned(&runtime).committed_mutation_blocks.is_empty(),
            "retained terminal evidence must not restore the published runtime fence"
        );
    }

    #[tokio::test]
    async fn peer_terminal_tombstone_gc_uses_etag_and_retains_racing_replacement() {
        use crate::services::tier::tier_mutation_intent::{
            save_tier_mutation_intent_record, tier_mutation_intent_record_object_name,
        };

        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (_, current_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should load with metadata");
        let current_etag = current_etag.expect("tier config fixture should have an ETag");
        let handle = TierConfigMgr::new();
        TierConfigMgr::reload_handle_with(&handle, store.clone())
            .await
            .expect("initial reload should publish the authoritative config");

        let mutation_id = uuid::Uuid::from_u128(0x153);
        let mut original = committed_remove_intent("COLD-A", mutation_id, &current_etag);
        original.candidate_digest = tier_config_candidate_digest(&persisted).expect("fixture digest should build");
        original.expires_at_unix_nanos = 1;
        save_tier_mutation_intent_record(store.clone(), &original)
            .await
            .expect("expired terminal fixture should persist");
        let mut replacement = original.clone();
        replacement.expires_at_unix_nanos = 2;
        let object = tier_mutation_intent_record_object_name(mutation_id).expect("peer record path should build");
        store
            .rewrite_on_next_if_match(object, replacement.encode().expect("replacement should encode"))
            .await;

        TierConfigMgr::reload_handle_with(&handle, store.clone())
            .await
            .expect("a tombstone ETag race should retain the replacement and retry later");

        let retained = TierConfigMgr::load_tier_mutation_intents(store)
            .await
            .expect("racing replacement should remain readable");
        assert_eq!(retained, vec![replacement]);
    }

    #[tokio::test]
    async fn quiescent_reload_scans_each_mutation_prefix_at_entry_and_final_race_check() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");

        TierConfigMgr::reload_handle_with(&TierConfigMgr::new(), store.clone())
            .await
            .expect("quiescent reload should publish the persisted config");

        assert_eq!(
            store.intent_list_calls(),
            4,
            "quiescent reload should scan the peer and coordinator prefixes once at entry and once before publish"
        );
    }

    #[tokio::test]
    async fn committed_replay_does_not_hold_local_admin_update_lock() {
        use crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record;

        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (_, current_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should load with metadata");
        let current_etag = current_etag.expect("tier config fixture should carry an ETag");
        let mutation_id = uuid::Uuid::from_u128(0x3f);
        let mut intent = committed_remove_intent("COLD-A", mutation_id, &current_etag);
        intent.candidate_digest = tier_config_candidate_digest(&persisted).expect("fixture digest should build");
        save_tier_mutation_intent_record(store.clone(), &intent)
            .await
            .expect("committed peer intent should persist");

        let handle = TierConfigMgr::new();
        let started = Arc::new(Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![Arc::new(BlockingCommitTierMutationPeer {
                    started: started.clone(),
                    release: release.clone(),
                })],
                async {
                    let reload = TierConfigMgr::reload_handle_with(&handle, store.clone());
                    tokio::pin!(reload);
                    tokio::select! {
                        result = &mut reload => panic!("reload finished before the peer commit was released: {result:?}"),
                        _ = started.notified() => {}
                    }

                    let update = tokio::time::timeout(Duration::from_secs(1), TierConfigMgr::admin_update_lock(&handle))
                        .await
                        .expect("peer commit replay must not hold the local admin update lock");
                    drop(update);
                    release.add_permits(1);
                    reload.await.expect("reload should finish after the peer commit is released");
                },
            )
            .await;
    }

    #[tokio::test]
    async fn reload_handle_recovers_two_committed_coordinator_mutations_for_same_tier() {
        let store = Arc::new(CasConfigStore::with_content_derived_etags());
        let mut first = empty_mgr();
        first.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        first
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("first tier config should persist");
        let (_, first_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("first tier config should load with metadata");

        let second = empty_mgr();
        let first_id = uuid::Uuid::from_u128(0x40);
        let mut first_intent = build_coordinator_tier_mutation_intent(
            TierMutationIntentKind::Remove,
            first_etag.clone(),
            &second,
            build_tier_mutation_affected_targets(
                TierMutationIntentKind::Remove,
                HashSet::from(["COLD-A".to_string()]),
                &first,
                &second,
            )
            .expect("first mutation targets should build"),
        )
        .expect("first coordinator intent should build")
        .expect("first mutation should require an intent");
        first_intent.mutation_id = first_id;
        save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &first_intent)
            .await
            .expect("first coordinator intent should persist");
        second
            .save_tiering_config_if_current(store.clone(), first_etag.as_deref())
            .await
            .expect("second tier config should persist");
        let (_, second_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("second tier config should load with metadata");
        let second_etag = second_etag.expect("second tier config should have an ETag");
        advance_tier_coordinator_mutation_intent_record_idempotent(
            store.clone(),
            first_id,
            TierMutationIntentState::Committed,
            Some(second_etag.clone()),
        )
        .await
        .expect("first coordinator intent should commit");

        let mut third = empty_mgr();
        third.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        let second_id = uuid::Uuid::from_u128(0x41);
        let mut second_intent = build_coordinator_tier_mutation_intent(
            TierMutationIntentKind::Add,
            Some(second_etag.clone()),
            &third,
            build_tier_mutation_affected_targets(
                TierMutationIntentKind::Add,
                HashSet::from(["COLD-A".to_string()]),
                &second,
                &third,
            )
            .expect("second mutation targets should build"),
        )
        .expect("second coordinator intent should build")
        .expect("second mutation should require an intent");
        second_intent.mutation_id = second_id;
        save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &second_intent)
            .await
            .expect("second coordinator intent should persist");
        third
            .save_tiering_config_if_current(store.clone(), Some(&second_etag))
            .await
            .expect("third tier config should persist");
        let (_, third_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("third tier config should load with metadata");
        let third_etag = third_etag.expect("third tier config should have an ETag");
        assert_eq!(
            Some(third_etag.as_str()),
            first_etag.as_deref(),
            "restoring identical tier config bytes must reuse the content-derived ETag"
        );
        advance_tier_coordinator_mutation_intent_record_idempotent(
            store.clone(),
            second_id,
            TierMutationIntentState::Committed,
            Some(third_etag.clone()),
        )
        .await
        .expect("second coordinator intent should commit");

        let calls = Arc::new(Mutex::new(Vec::new()));
        let handle = TierConfigMgr::new();
        store
            .fail_delete_with_prefix_after(
                crate::services::tier::tier_mutation_intent::TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX,
                1,
            )
            .await;
        let err = TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async { TierConfigMgr::reload_handle_with(&handle, store.clone()).await },
            )
            .await
            .expect_err("a tail cleanup failure must fail the first reload");
        assert!(err.to_string().contains("injected tier mutation intent delete failure"), "{err}");
        let remaining = TierConfigMgr::load_coordinator_mutation_intents(store.clone())
            .await
            .expect("coordinator scan should succeed after partial cleanup");
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].mutation_id, second_id, "the current chain tail must be deleted last");

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::reload_handle_with(&handle, store.clone())
                        .await
                        .expect("a second reload should finish tail cleanup");
                },
            )
            .await;

        assert_eq!(
            lock_unpoisoned(&calls).as_slice(),
            &[
                format!("peer-a:commit:{second_id}:{third_etag}"),
                format!("peer-a:commit:{second_id}:{third_etag}"),
            ],
            "only the chain tail requires a missing-record peer proof on each retry"
        );
        assert!(handle.read().await.tiers.contains_key("COLD-A"));
        assert!(
            TierConfigMgr::load_coordinator_mutation_intents(store.clone())
                .await
                .expect("coordinator scan should succeed after cleanup")
                .is_empty()
        );
        let guard = handle.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
        assert!(lock_unpoisoned(&runtime).committed_mutation_blocks.is_empty());
    }

    #[test]
    fn dual_prefix_mutation_intents_merge_only_valid_terminal_orders() {
        let mutation_id = uuid::Uuid::from_u128(0x50);
        let prepared = prepared_remove_intent("COLD-A", mutation_id);
        let mut committed = prepared.clone();
        committed
            .advance(TierMutationIntentState::Committed, Some("etag-new".to_string()))
            .expect("committed fixture should advance");
        let mut aborted = prepared.clone();
        aborted
            .advance(TierMutationIntentState::Aborted, None)
            .expect("aborted fixture should advance");

        for (peer, coordinator, expected) in [
            (prepared.clone(), committed.clone(), committed.clone()),
            (prepared.clone(), aborted.clone(), aborted.clone()),
            (aborted.clone(), prepared.clone(), aborted.clone()),
            (aborted.clone(), committed.clone(), committed.clone()),
            (committed.clone(), committed.clone(), committed.clone()),
            (aborted.clone(), aborted.clone(), aborted.clone()),
        ] {
            let merged = TierConfigMgr::merge_mutation_recovery_intents(&[peer], &[coordinator])
                .expect("a prepared record and its matching terminal state should merge");
            assert_eq!(merged.len(), 1);
            assert_eq!(merged[0].intent, expected);
            assert!(merged[0].has_peer_record && merged[0].has_coordinator_record);
        }

        let err =
            TierConfigMgr::merge_mutation_recovery_intents(std::slice::from_ref(&committed), std::slice::from_ref(&prepared))
                .expect_err("a peer committed record cannot outrun the coordinator commit order");
        assert!(err.to_string().contains("conflicting states"), "{err}");

        let mut conflicting_identity = prepared.clone();
        conflicting_identity.affected_targets[0].tier_name = "COLD-B".to_string();
        let err = TierConfigMgr::merge_mutation_recovery_intents(&[prepared], &[conflicting_identity])
            .expect_err("same UUID with a conflicting target must fail closed");
        assert!(err.to_string().contains("conflicting identities"), "{err}");

        let mut conflicting_commit = committed.clone();
        conflicting_commit.committed_config_etag = Some("other-etag".to_string());
        let err = TierConfigMgr::merge_mutation_recovery_intents(&[committed.clone()], &[conflicting_commit])
            .expect_err("same UUID with conflicting committed ETags must fail closed");
        assert!(err.to_string().contains("conflicting states"), "{err}");

        let err = TierConfigMgr::merge_mutation_recovery_intents(&[committed], &[aborted])
            .expect_err("committed and aborted records must not merge");
        assert!(err.to_string().contains("conflicting states"), "{err}");
    }

    #[tokio::test]
    async fn aborted_peer_and_committed_coordinator_recover_as_committed() {
        use crate::services::tier::tier_mutation_intent::{
            TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX, TIER_MUTATION_INTENT_RECORD_PREFIX, save_tier_mutation_intent_record,
        };

        let store = Arc::new(CasConfigStore::default());
        let base = empty_mgr();
        base.save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("base tier config should persist");
        let (_, base_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("base tier config should load with metadata");

        let mut candidate = empty_mgr();
        candidate.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        let mutation_id = uuid::Uuid::from_u128(0x53);
        let mut prepared = build_coordinator_tier_mutation_intent(
            TierMutationIntentKind::Add,
            base_etag.clone(),
            &candidate,
            build_tier_mutation_affected_targets(
                TierMutationIntentKind::Add,
                HashSet::from(["COLD-A".to_string()]),
                &base,
                &candidate,
            )
            .expect("add mutation targets should build"),
        )
        .expect("coordinator intent should build")
        .expect("add mutation should require an intent");
        prepared.mutation_id = mutation_id;
        save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &prepared)
            .await
            .expect("prepared coordinator intent should persist");

        candidate
            .save_tiering_config_if_current(store.clone(), base_etag.as_deref())
            .await
            .expect("candidate tier config should persist");
        let (_, committed_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("candidate tier config should load with metadata");
        let committed_etag = committed_etag.expect("candidate tier config should carry an ETag");
        advance_tier_coordinator_mutation_intent_record_idempotent(
            store.clone(),
            mutation_id,
            TierMutationIntentState::Committed,
            Some(committed_etag.clone()),
        )
        .await
        .expect("coordinator intent should commit");

        let mut aborted_peer = prepared.clone();
        aborted_peer
            .advance(TierMutationIntentState::Aborted, None)
            .expect("peer fixture should abort after its late RPC");
        save_tier_mutation_intent_record(store.clone(), &aborted_peer)
            .await
            .expect("aborted peer intent should persist");

        let handle = TierConfigMgr::new();
        TierConfigMgr::apply_prepared_mutation_intent_block(&handle, &prepared)
            .await
            .expect("prepared peer fence should be installed");
        let calls = Arc::new(Mutex::new(Vec::new()));
        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::reload_handle_with(&handle, store.clone())
                        .await
                        .expect("committed coordinator proof should override the stale aborted peer record");
                },
            )
            .await;

        assert_eq!(
            lock_unpoisoned(&calls).as_slice(),
            &[format!("peer-a:commit:{mutation_id}:{committed_etag}")]
        );
        assert!(handle.read().await.tiers.contains_key("COLD-A"));
        assert!(
            TierConfigMgr::load_tier_mutation_intents(store.clone())
                .await
                .expect("peer scan should succeed after recovery")
                .is_empty()
        );
        assert!(
            TierConfigMgr::load_coordinator_mutation_intents(store.clone())
                .await
                .expect("coordinator scan should succeed after recovery")
                .is_empty()
        );
        let delete_log = store.delete_log().await;
        assert!(
            delete_log
                .iter()
                .any(|object| object.starts_with(TIER_MUTATION_INTENT_RECORD_PREFIX))
        );
        assert!(
            delete_log
                .iter()
                .any(|object| object.starts_with(TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX))
        );
        drop(
            TierConfigMgr::acquire_operation_lease(&handle, "COLD-A")
                .await
                .expect("successful committed recovery should clear every runtime fence"),
        );
    }

    #[tokio::test]
    async fn aborted_dual_prefix_recovery_converges_after_peer_cleanup_failure() {
        use crate::services::tier::tier_mutation_intent::{TIER_MUTATION_INTENT_RECORD_PREFIX, save_tier_mutation_intent_record};

        for (case, peer_is_terminal) in [("peer-terminal", true), ("coordinator-terminal", false)] {
            let store = Arc::new(CasConfigStore::default());
            let mut persisted = empty_mgr();
            persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
            persisted
                .save_tiering_config_if_current(store.clone(), None)
                .await
                .expect("tier config fixture should persist");

            let mutation_id = if peer_is_terminal {
                uuid::Uuid::from_u128(0x51)
            } else {
                uuid::Uuid::from_u128(0x52)
            };
            let prepared = prepared_remove_intent("COLD-A", mutation_id);
            let mut aborted = prepared.clone();
            aborted
                .advance(TierMutationIntentState::Aborted, None)
                .expect("aborted fixture should advance");
            let (peer, coordinator) = if peer_is_terminal {
                (aborted, prepared)
            } else {
                (prepared, aborted)
            };
            save_tier_mutation_intent_record(store.clone(), &peer)
                .await
                .expect("peer intent fixture should persist");
            save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &coordinator)
                .await
                .expect("coordinator intent fixture should persist");
            store.fail_next_delete_with_prefix(TIER_MUTATION_INTENT_RECORD_PREFIX).await;

            let calls = Arc::new(Mutex::new(Vec::new()));
            let handle = TierConfigMgr::new();
            let err = TIER_MUTATION_TEST_PEERS
                .scope(
                    vec![FakeTierMutationPeer::boxed(
                        "peer-a",
                        calls.clone(),
                        Ok(PeerTierMutationState::Committed),
                    )],
                    async { TierConfigMgr::reload_handle_with(&handle, store.clone()).await },
                )
                .await
                .expect_err("injected peer cleanup failure must fail the first reload");
            assert!(err.to_string().contains("injected tier mutation intent delete failure"), "{case}: {err}");
            assert_eq!(
                lock_unpoisoned(&calls).as_slice(),
                &[format!("peer-a:abort:{mutation_id}")],
                "{case}: terminal abort recovery must clear every peer runtime fence"
            );
            assert!(
                TierConfigMgr::load_coordinator_mutation_intents(store.clone())
                    .await
                    .expect("coordinator scan should succeed after cleanup")
                    .is_empty(),
                "{case}: coordinator evidence should be cleaned first"
            );
            let remaining = TierConfigMgr::load_tier_mutation_intents(store.clone())
                .await
                .expect("peer scan should retain the failed cleanup record");
            assert_eq!(remaining.len(), 1, "{case}");
            assert_eq!(remaining[0].state, TierMutationIntentState::Aborted, "{case}");

            TierConfigMgr::reload_handle_with(&handle, store.clone())
                .await
                .expect("peer-only aborted evidence should clean up on retry");
            assert!(
                TierConfigMgr::load_tier_mutation_intents(store)
                    .await
                    .expect("peer scan should succeed after retry")
                    .is_empty(),
                "{case}: retry should remove the terminal peer record"
            );
            let guard = handle.read().await;
            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
            assert!(lock_unpoisoned(&runtime).prepared_mutation_blocks.is_empty(), "{case}");
        }
    }

    #[tokio::test]
    async fn committed_recovery_plan_accepts_etag_reuse_and_orders_current_proofs_first() {
        fn edge(id: u128, old: &str, new: &str, peer: bool, coordinator: bool) -> RecoveredTierMutationIntent {
            let mut intent = committed_remove_intent("COLD-A", uuid::Uuid::from_u128(id), new);
            intent.old_config_etag = Some(old.to_string());
            RecoveredTierMutationIntent {
                intent,
                has_peer_record: peer,
                has_coordinator_record: coordinator,
                peer_state: peer.then_some(TierMutationIntentState::Committed),
                coordinator_state: coordinator.then_some(TierMutationIntentState::Committed),
            }
        }

        fn snapshot(intents: Vec<RecoveredTierMutationIntent>) -> TierMutationRecoverySnapshot {
            let mutation_ids = intents.iter().map(|recovered| recovered.intent.mutation_id).collect();
            let has_committed_mutation_blocks = !intents.is_empty();
            TierMutationRecoverySnapshot {
                coordinator_intents: intents
                    .iter()
                    .filter(|recovered| recovered.has_coordinator_record)
                    .map(|recovered| recovered.intent.clone())
                    .collect(),
                intents,
                allowance: MutationBlockAllowance {
                    mutation_ids,
                    revision: 0,
                },
                has_committed_mutation_blocks,
            }
        }

        let noop_id = uuid::Uuid::from_u128(9);
        let plan = TierConfigMgr::plan_committed_mutation_recovery(
            &snapshot(vec![edge(9, "etag-a", "etag-a", false, true)]),
            Some("etag-a"),
        )
        .expect("a valid no-op rewrite must not look like a self-loop");
        assert_eq!(plan.replay_order, vec![noop_id]);

        let old_id = uuid::Uuid::from_u128(10);
        let tail_id = uuid::Uuid::from_u128(11);
        let plan = TierConfigMgr::plan_committed_mutation_recovery(
            &snapshot(vec![
                edge(10, "etag-a", "etag-b", false, true),
                edge(11, "etag-b", "etag-a", false, true),
            ]),
            Some("etag-a"),
        )
        .expect("content ETag reuse must not make valid A-to-B-to-A history look corrupt");
        assert_eq!(plan.replay_order, vec![tail_id]);
        assert_eq!(plan.coordinator_cleanup_order, vec![old_id, tail_id]);

        let first_current_id = uuid::Uuid::from_u128(20);
        let second_current_id = uuid::Uuid::from_u128(21);
        let plan = TierConfigMgr::plan_committed_mutation_recovery(
            &snapshot(vec![
                edge(20, "etag-a", "etag-current", false, true),
                edge(21, "etag-b", "etag-current", false, true),
            ]),
            Some("etag-current"),
        )
        .expect("multiple coordinator proofs for repeated current content must remain replayable");
        assert_eq!(plan.replay_order, vec![first_current_id, second_current_id]);

        let stale_peer_id = uuid::Uuid::from_u128(1);
        let current_proof_id = uuid::Uuid::from_u128(99);
        let mixed_snapshot = snapshot(vec![
            edge(1, "etag-old", "etag-stale", true, false),
            edge(99, "etag-previous", "etag-current", false, true),
        ]);
        let plan = TierConfigMgr::plan_committed_mutation_recovery(&mixed_snapshot, Some("etag-current"))
            .expect("a disconnected peer record must not invalidate the current coordinator proof");
        assert_eq!(plan.replay_order, vec![current_proof_id, stale_peer_id]);
        let calls = Arc::new(Mutex::new(Vec::new()));
        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::replay_recovered_committed_mutation_intents(&mixed_snapshot, &plan)
                        .await
                        .expect("the current config proof should replay before disconnected peer history");
                },
            )
            .await;
        assert_eq!(
            lock_unpoisoned(&calls).as_slice(),
            &[
                format!("peer-a:commit:{current_proof_id}:etag-current"),
                format!("peer-a:commit:{stale_peer_id}:etag-stale"),
            ]
        );

        let first_gap_id = uuid::Uuid::from_u128(30);
        let second_gap_id = uuid::Uuid::from_u128(31);
        let plan = TierConfigMgr::plan_committed_mutation_recovery(
            &snapshot(vec![
                edge(31, "etag-c", "etag-d", true, false),
                edge(30, "etag-a", "etag-b", true, false),
            ]),
            Some("etag-current"),
        )
        .expect("historical peer-only gaps must replay from their persisted records");
        assert_eq!(plan.replay_order, vec![first_gap_id, second_gap_id]);
        assert_eq!(plan.peer_cleanup_order, vec![first_gap_id, second_gap_id]);

        let err =
            TierConfigMgr::plan_committed_mutation_recovery(&snapshot(vec![edge(40, "etag-a", "etag-b", true, true)]), None)
                .expect_err("every committed recovery path requires the authoritative current ETag");
        assert!(err.to_string().contains("requires the current config ETag"), "{err}");
    }

    #[tokio::test]
    async fn committed_mutation_allowance_requires_every_same_tier_id() {
        let manager = TierConfigMgr::new();
        manager
            .write()
            .await
            .tiers
            .insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        let first = committed_remove_intent("COLD-A", uuid::Uuid::from_u128(0x60), "etag-one");
        let second = committed_remove_intent("COLD-A", uuid::Uuid::from_u128(0x61), "etag-two");
        TierConfigMgr::apply_committed_mutation_intent_block(&manager, &first)
            .await
            .expect("first committed block should apply");
        TierConfigMgr::apply_committed_mutation_intent_block(&manager, &second)
            .await
            .expect("second committed block should apply");
        let revision = TierConfigMgr::mutation_blocks_revision(&manager).await;
        let update = TierConfigMgr::admin_update_lock(&manager).await;
        let err = TierConfigMgr::publish_candidate_owned_with_allowed_mutation_blocks(
            &manager,
            empty_mgr(),
            None,
            update,
            Some(MutationBlockAllowance {
                mutation_ids: HashSet::from([first.mutation_id]),
                revision,
            }),
        )
        .await
        .expect_err("an allowance missing one committed ID must not publish");
        assert!(err.message.contains("already being replaced"), "{err}");
        assert!(manager.read().await.tiers.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn committed_cleanup_is_coordinator_first_and_retries_after_each_phase_failure() {
        use crate::services::tier::tier_mutation_intent::{
            TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX, TIER_MUTATION_INTENT_RECORD_PREFIX, save_tier_mutation_intent_record,
        };

        async fn install_dual_committed_fixture(store: Arc<CasConfigStore>, mutation_id: uuid::Uuid) -> String {
            let mut persisted = empty_mgr();
            persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
            persisted
                .save_tiering_config_if_current(store.clone(), None)
                .await
                .expect("tier config fixture should persist");
            let (_, current_etag) = load_tier_config_for_update(store.clone())
                .await
                .expect("tier config fixture should load with metadata");
            let current_etag = current_etag.expect("tier config fixture should have an ETag");
            let mut intent = committed_remove_intent("COLD-A", mutation_id, &current_etag);
            intent.old_config_etag = Some("previous-etag".to_string());
            intent.candidate_digest = tier_config_candidate_digest(&persisted).expect("fixture digest should build");
            save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &intent)
                .await
                .expect("coordinator fixture should persist");
            save_tier_mutation_intent_record(store, &intent)
                .await
                .expect("peer fixture should persist");
            current_etag
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let store = Arc::new(CasConfigStore::default());
        let first_id = uuid::Uuid::from_u128(0x70);
        let first_etag = install_dual_committed_fixture(store.clone(), first_id).await;
        store
            .fail_next_delete_with_prefix(TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX)
            .await;
        let first_handle = TierConfigMgr::new();
        let err = TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async { TierConfigMgr::reload_handle_with(&first_handle, store.clone()).await },
            )
            .await
            .expect_err("coordinator cleanup failure must fail the reload");
        assert!(err.to_string().contains("injected tier mutation intent delete failure"), "{err}");
        {
            let guard = first_handle.read().await;
            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
            assert!(
                lock_unpoisoned(&runtime)
                    .committed_mutation_blocks
                    .get("COLD-A")
                    .is_some_and(|ids| ids.contains(&first_id))
            );
        }
        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::reload_handle_with(&first_handle, store.clone())
                        .await
                        .expect("second reload should finish coordinator cleanup");
                },
            )
            .await;

        let second_store = Arc::new(CasConfigStore::default());
        let second_id = uuid::Uuid::from_u128(0x71);
        let second_etag = install_dual_committed_fixture(second_store.clone(), second_id).await;
        second_store
            .fail_next_delete_with_prefix(TIER_MUTATION_INTENT_RECORD_PREFIX)
            .await;
        let second_handle = TierConfigMgr::new();
        let err = TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async { TierConfigMgr::reload_handle_with(&second_handle, second_store.clone()).await },
            )
            .await
            .expect_err("peer cleanup failure must fail the reload");
        assert!(err.to_string().contains("injected tier mutation intent delete failure"), "{err}");
        let delete_log = second_store.delete_log().await;
        let coordinator_delete = delete_log
            .iter()
            .position(|object| object.starts_with(TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX))
            .expect("coordinator cleanup should be attempted");
        let peer_delete = delete_log
            .iter()
            .position(|object| object.starts_with(TIER_MUTATION_INTENT_RECORD_PREFIX))
            .expect("peer cleanup should be attempted");
        assert!(
            coordinator_delete < peer_delete,
            "coordinator evidence must be deleted first: {delete_log:?}"
        );
        {
            let guard = second_handle.read().await;
            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
            assert!(
                lock_unpoisoned(&runtime).committed_mutation_blocks.is_empty(),
                "a failed tombstone GC must not restore a fence after the authoritative config was published"
            );
        }
        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::reload_handle_with(&second_handle, second_store.clone())
                        .await
                        .expect("second reload should finish peer cleanup");
                },
            )
            .await;

        {
            let calls = lock_unpoisoned(&calls);
            assert!(calls.contains(&format!("peer-a:commit:{first_id}:{first_etag}")));
            assert!(calls.contains(&format!("peer-a:commit:{second_id}:{second_etag}")));
        }
        assert!(
            TierConfigMgr::load_tier_mutation_intents(second_store)
                .await
                .expect("peer scan should succeed after retry")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn coordinator_terminal_cleanup_uses_etag_and_retains_racing_replacement() {
        use crate::services::tier::tier_mutation_intent::{
            TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX, TIER_MUTATION_INTENT_RECORD_PREFIX, save_tier_mutation_intent_record,
            tier_mutation_intent_record_object_name,
        };

        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (_, current_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should load with metadata");
        let current_etag = current_etag.expect("tier config fixture should have an ETag");
        let mutation_id = uuid::Uuid::from_u128(0x154);
        let mut intent = committed_remove_intent("COLD-A", mutation_id, &current_etag);
        intent.old_config_etag = Some("previous-etag".to_string());
        intent.candidate_digest = tier_config_candidate_digest(&persisted).expect("fixture digest should build");
        save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &intent)
            .await
            .expect("coordinator fixture should persist");
        save_tier_mutation_intent_record(store.clone(), &intent)
            .await
            .expect("peer fixture should persist");

        let peer_object = tier_mutation_intent_record_object_name(mutation_id).expect("peer record path should build");
        let coordinator_object =
            peer_object.replacen(TIER_MUTATION_INTENT_RECORD_PREFIX, TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX, 1);
        let mut replacement = intent.clone();
        replacement.expires_at_unix_nanos = replacement.expires_at_unix_nanos.saturating_add(1);
        store
            .rewrite_on_next_if_match(coordinator_object, replacement.encode().expect("coordinator replacement should encode"))
            .await;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let handle = TierConfigMgr::new();
        let err = TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls,
                    Ok(PeerTierMutationState::Committed),
                )],
                async { TierConfigMgr::reload_handle_with(&handle, store.clone()).await },
            )
            .await
            .expect_err("a coordinator ETag race must fail closed");
        assert!(err.to_string().contains("changed during conditional cleanup"), "{err}");
        assert_eq!(
            TierConfigMgr::load_coordinator_mutation_intents(store.clone())
                .await
                .expect("coordinator replacement should remain readable"),
            vec![replacement]
        );
        assert_eq!(
            TierConfigMgr::load_tier_mutation_intents(store)
                .await
                .expect("peer terminal should remain while coordinator cleanup is unresolved"),
            vec![intent]
        );
    }

    #[tokio::test]
    async fn peer_only_committed_aba_retries_partial_cleanup() {
        use crate::services::tier::tier_mutation_intent::{
            TIER_MUTATION_INTENT_RECORD_PREFIX, save_tier_mutation_intent_record, tier_mutation_intent_record_object_name,
        };

        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (_, current_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should load with metadata");
        let current_etag = current_etag.expect("tier config fixture should have an ETag");

        let first_id = uuid::Uuid::from_u128(0x72);
        let second_id = uuid::Uuid::from_u128(0x73);
        let mut first = committed_remove_intent("COLD-A", first_id, "intermediate-etag");
        first.old_config_etag = Some(current_etag.clone());
        let mut second = committed_remove_intent("COLD-A", second_id, &current_etag);
        second.old_config_etag = Some("intermediate-etag".to_string());
        second.candidate_digest = tier_config_candidate_digest(&persisted).expect("fixture digest should build");
        for intent in [&first, &second] {
            save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), intent)
                .await
                .expect("coordinator fixture should persist");
            save_tier_mutation_intent_record(store.clone(), intent)
                .await
                .expect("peer fixture should persist");
        }

        store.fail_next_delete_with_prefix(TIER_MUTATION_INTENT_RECORD_PREFIX).await;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let handle = TierConfigMgr::new();
        let err = TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async { TierConfigMgr::reload_handle_with(&handle, store.clone()).await },
            )
            .await
            .expect_err("the injected first peer cleanup failure must fail reload");
        assert!(err.to_string().contains("injected tier mutation intent delete failure"), "{err}");
        assert!(
            TierConfigMgr::load_coordinator_mutation_intents(store.clone())
                .await
                .expect("coordinator scan should succeed after cleanup")
                .is_empty(),
            "coordinator evidence should be removed before peer cleanup"
        );
        let remaining = TierConfigMgr::load_tier_mutation_intents(store.clone())
            .await
            .expect("both peer records should remain after the first delete fails");
        assert_eq!(
            remaining.iter().map(|intent| intent.mutation_id).collect::<HashSet<_>>(),
            HashSet::from([first_id, second_id])
        );

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::reload_handle_with(&handle, store.clone())
                        .await
                        .expect("peer-only committed ABA records should converge on retry");
                },
            )
            .await;

        assert!(
            TierConfigMgr::load_tier_mutation_intents(store.clone())
                .await
                .expect("peer scan should succeed after retry")
                .is_empty()
        );
        let first_object = tier_mutation_intent_record_object_name(first_id).expect("first peer record path should build");
        let second_object = tier_mutation_intent_record_object_name(second_id).expect("second peer record path should build");
        let peer_deletes = store
            .delete_log()
            .await
            .into_iter()
            .filter(|object| object.starts_with(TIER_MUTATION_INTENT_RECORD_PREFIX))
            .collect::<Vec<_>>();
        assert_eq!(peer_deletes, vec![first_object.clone(), second_object, first_object]);
        assert_eq!(
            lock_unpoisoned(&calls).as_slice(),
            &[
                format!("peer-a:commit:{first_id}:intermediate-etag"),
                format!("peer-a:commit:{second_id}:{current_etag}"),
                format!("peer-a:commit:{first_id}:intermediate-etag"),
            ]
        );
    }

    #[tokio::test]
    async fn reload_handle_replays_committed_and_restores_prepared_blocks() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted.tiers.insert("COLD-B".to_string(), build_rustfs_tier("COLD-B"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");

        let committed_id = uuid::Uuid::from_u128(18);
        let committed = committed_remove_intent("COLD-A", committed_id, "etag-new");
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &committed)
            .await
            .expect("committed intent fixture should persist");
        let prepared_id = uuid::Uuid::from_u128(19);
        let prepared = prepared_remove_intent("COLD-B", prepared_id);
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &prepared)
            .await
            .expect("prepared intent fixture should persist");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let handle = TierConfigMgr::new();

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::reload_handle_with(&handle, store.clone())
                        .await
                        .expect("reload should replay committed intent and restore prepared blocks");
                },
            )
            .await;

        assert_eq!(lock_unpoisoned(&calls).as_slice(), &[format!("peer-a:commit:{committed_id}:etag-new")]);
        let err = match TierConfigMgr::acquire_operation_lease(&handle, "COLD-B").await {
            Ok(_) => panic!("prepared intent must still block operation leases after reload"),
            Err(err) => err,
        };
        assert!(err.message.contains("being replaced"), "{err}");
        let reloaded = TierConfigMgr::load_tier_mutation_intents(store)
            .await
            .expect("intent scan should succeed after mixed reload");
        assert!(reloaded.iter().all(|intent| intent.mutation_id != committed_id));
        assert!(reloaded.iter().any(|intent| intent.mutation_id == prepared_id));
    }

    #[tokio::test]
    async fn reload_handle_keeps_committed_block_until_publish_swaps_generation() {
        let store = Arc::new(CasConfigStore::default());
        empty_mgr()
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("empty tier config fixture should persist");

        let first_id = uuid::Uuid::from_u128(21);
        let first = committed_remove_intent("COLD-A", first_id, "etag-one");
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &first)
            .await
            .expect("first committed intent fixture should persist");
        let second_id = uuid::Uuid::from_u128(22);
        let second = committed_remove_intent("COLD-A", second_id, "etag-two");
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &second)
            .await
            .expect("second committed intent fixture should persist");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let handle = TierConfigMgr::new();
        {
            let mut guard = handle.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&handle, "COLD-A")
            .await
            .expect("old generation lease should be available before recovery");

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    let reload_handle = handle.clone();
                    let reload_store = store.clone();
                    let reload = async move { TierConfigMgr::reload_handle_with(&reload_handle, reload_store).await };
                    let probe_handle = handle.clone();
                    let missing_record_store = store.clone();
                    let probe = async move {
                        tokio::time::timeout(Duration::from_secs(1), async {
                            while old.inner.accepting.load(Ordering::Acquire) {
                                tokio::task::yield_now().await;
                            }
                        })
                        .await
                        .expect("reload publish should revoke the old generation before waiting");
                        {
                            let guard = probe_handle.read().await;
                            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
                            assert!(
                                lock_unpoisoned(&runtime)
                                    .committed_mutation_blocks
                                    .get("COLD-A")
                                    .is_some_and(|mutation_ids| { mutation_ids == &HashSet::from([first_id, second_id]) }),
                                "both committed replay blocks must remain until the local publish completes"
                            );
                        }
                        let blocked = match TierConfigMgr::acquire_operation_lease(&probe_handle, "COLD-A").await {
                            Ok(_) => panic!("committed replay must block new old-generation leases while publish waits"),
                            Err(err) => err,
                        };
                        assert!(blocked.message.contains("being replaced"), "{blocked}");

                        crate::services::tier::tier_mutation_intent::delete_tier_mutation_intent_record(
                            missing_record_store.clone(),
                            first_id,
                        )
                        .await
                        .expect("peer may remove the first durable committed record while publish is waiting");
                        crate::services::tier::tier_mutation_intent::delete_tier_mutation_intent_record(
                            missing_record_store.clone(),
                            second_id,
                        )
                        .await
                        .expect("peer may remove the second durable committed record while publish is waiting");
                        TierConfigMgr::load_and_reconcile_mutation_recovery_snapshot(
                            &probe_handle,
                            missing_record_store.clone(),
                            true,
                        )
                        .await
                        .expect("missing durable committed records must not clear the runtime fence");
                        {
                            let guard = probe_handle.read().await;
                            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
                            assert_eq!(
                                lock_unpoisoned(&runtime).committed_mutation_blocks.get("COLD-A"),
                                Some(&HashSet::from([first_id, second_id])),
                                "a missing durable record must retain committed blocks until publish completes"
                            );
                        }
                        drop(old);
                    };
                    let (reload_result, ()) = tokio::join!(reload, probe);
                    reload_result.expect("reload should complete after the old lease drains");
                },
            )
            .await;

        assert_eq!(
            lock_unpoisoned(&calls).as_slice(),
            &[
                format!("peer-a:commit:{first_id}:etag-one"),
                format!("peer-a:commit:{second_id}:etag-two"),
            ]
        );
        assert!(
            !handle.read().await.tiers.contains_key("COLD-A"),
            "reloaded manager should publish the committed removal after the old lease drains"
        );
        let reloaded = TierConfigMgr::load_tier_mutation_intents(store)
            .await
            .expect("intent scan should succeed after committed removal reload");
        assert!(
            reloaded
                .iter()
                .all(|intent| intent.mutation_id != first_id && intent.mutation_id != second_id)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn reload_handle_keeps_committed_intent_when_local_publish_fails() {
        let store = Arc::new(CasConfigStore::default());
        empty_mgr()
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("empty tier config fixture should persist");

        let mutation_id = uuid::Uuid::from_u128(22);
        let intent = committed_remove_intent("COLD-A", mutation_id, "etag-new");
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &intent)
            .await
            .expect("committed intent fixture should persist");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let handle = TierConfigMgr::new();
        {
            let mut guard = handle.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&handle, "COLD-A")
            .await
            .expect("old generation lease should be available before recovery");

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    let reload_handle = handle.clone();
                    let reload_store = store.clone();
                    let reload = async move { TierConfigMgr::reload_handle_with(&reload_handle, reload_store).await };
                    let probe = async {
                        while old.inner.accepting.load(Ordering::Acquire) {
                            tokio::task::yield_now().await;
                        }
                        tokio::time::advance(TIER_OPERATION_DRAIN_TIMEOUT).await;
                    };
                    let (reload_result, ()) = tokio::join!(reload, probe);
                    let err = reload_result.expect_err("reload must fail while the old lease refuses to drain");
                    assert!(
                        err.to_string()
                            .contains("Timed out waiting for active remote tier operations"),
                        "{err}"
                    );
                },
            )
            .await;

        assert_eq!(lock_unpoisoned(&calls).as_slice(), &[format!("peer-a:commit:{mutation_id}:etag-new")]);
        let reloaded = TierConfigMgr::load_tier_mutation_intents(store)
            .await
            .expect("intent scan should succeed after failed local publish");
        assert!(
            reloaded.iter().any(|intent| intent.mutation_id == mutation_id),
            "failed local publish must keep the committed intent for retry"
        );
        {
            let guard = handle.read().await;
            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
            assert!(
                lock_unpoisoned(&runtime)
                    .committed_mutation_blocks
                    .get("COLD-A")
                    .is_some_and(|mutation_ids| mutation_ids.contains(&mutation_id))
            );
        }
        let blocked = match TierConfigMgr::acquire_operation_lease(&handle, "COLD-A").await {
            Ok(_) => panic!("failed local publish must keep blocking old-generation leases"),
            Err(err) => err,
        };
        assert!(blocked.message.contains("being replaced"), "{blocked}");
        drop(old);
    }

    #[tokio::test]
    async fn committed_replay_claim_prevents_duplicate_reload_fanout() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");

        let mutation_id = uuid::Uuid::from_u128(20);
        let intent = committed_remove_intent("COLD-A", mutation_id, "etag-new");
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &intent)
            .await
            .expect("committed intent fixture should persist");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let left = TierConfigMgr::new();
        let right = TierConfigMgr::new();

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    let (left_result, right_result) = tokio::join!(
                        TierConfigMgr::reload_handle_with(&left, store.clone()),
                        TierConfigMgr::reload_handle_with(&right, store.clone())
                    );
                    left_result.expect("first reload should complete");
                    right_result.expect("second reload should observe the cleaned committed intent");
                },
            )
            .await;

        assert_eq!(
            lock_unpoisoned(&calls).as_slice(),
            &[format!("peer-a:commit:{mutation_id}:etag-new")],
            "only the claimed reload should fan out committed replay"
        );
    }

    #[tokio::test]
    async fn coordinator_mutation_recovery_scan_blocks_new_operation_lease() {
        let store = Arc::new(CasConfigStore::default());
        let mutation_id = uuid::Uuid::from_u128(23);
        let intent = prepared_remove_intent("COLD-A", mutation_id);
        save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &intent)
            .await
            .expect("coordinator intent fixture should persist");

        let loaded_prepared = TierConfigMgr::load_prepared_coordinator_mutation_intents(store.clone())
            .await
            .expect("coordinator intent scan should load prepared record");
        assert_eq!(loaded_prepared.len(), 1);
        assert_eq!(loaded_prepared[0].mutation_id, mutation_id);

        let manager = TierConfigMgr::new();
        TierConfigMgr::reconcile_prepared_mutation_intents_from_store(&manager, store)
            .await
            .expect("coordinator prepared recovery should reconcile");
        let err = match TierConfigMgr::acquire_operation_lease(&manager, "COLD-A").await {
            Ok(_) => panic!("coordinator prepared intent must block new operation leases"),
            Err(err) => err,
        };
        assert!(err.message.contains("being replaced"), "{err}");
    }

    #[tokio::test]
    async fn coordinator_mutation_recovery_commits_saved_config_digest() {
        let store = Arc::new(CasConfigStore::default());
        let mut current = empty_mgr();
        current.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        current
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("current tier config fixture should persist");
        let (_, old_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("current tier config fixture should have metadata");

        let candidate = empty_mgr();
        let mut intent = build_coordinator_tier_mutation_intent(
            TierMutationIntentKind::Remove,
            old_etag.clone(),
            &candidate,
            build_tier_mutation_affected_targets(
                TierMutationIntentKind::Remove,
                HashSet::from(["COLD-A".to_string()]),
                &current,
                &candidate,
            )
            .expect("remove fixture should produce an affected target"),
        )
        .expect("coordinator intent fixture should build")
        .expect("remove fixture should require a coordinator intent");
        intent.expires_at_unix_nanos = 1;
        save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &intent)
            .await
            .expect("coordinator intent fixture should persist");
        let mut aborted = prepared_remove_intent("COLD-Z", uuid::Uuid::from_u128(26));
        aborted
            .advance(TierMutationIntentState::Aborted, None)
            .expect("aborted coordinator fixture should advance");
        save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &aborted)
            .await
            .expect("aborted coordinator fixture should persist");
        candidate
            .save_tiering_config_if_current(store.clone(), old_etag.as_deref())
            .await
            .expect("candidate fixture should persist before coordinator commit");

        let handle = TierConfigMgr::new();
        TierConfigMgr::reload_handle_with(&handle, store.clone())
            .await
            .expect("reload should commit coordinator intent whose candidate digest is already saved");

        assert!(
            !handle.read().await.tiers.contains_key("COLD-A"),
            "reload should publish the already saved coordinator candidate"
        );
        let reloaded = TierConfigMgr::load_coordinator_mutation_intents(store)
            .await
            .expect("coordinator intent scan should succeed after recovery cleanup");
        assert!(
            reloaded.iter().all(|record| record.mutation_id != intent.mutation_id),
            "committed coordinator intent should be removed after local publish"
        );
        assert!(
            reloaded.iter().all(|record| record.mutation_id != aborted.mutation_id),
            "aborted coordinator intent should be removed after local publish"
        );
    }

    #[tokio::test]
    async fn coordinator_mutation_recovery_accepts_hashmap_encoded_persisted_digest() {
        let store = Arc::new(CasConfigStore::default());
        let mut candidate = empty_mgr();
        for tier_name in ["COLD-B", "COLD-Z"] {
            candidate.tiers.insert(tier_name.to_string(), build_rustfs_tier(tier_name));
        }
        let mut payload = Vec::new();
        rmp::encode::write_array_len(&mut payload, 1).expect("legacy config wrapper should encode");
        rmp::encode::write_map_len(&mut payload, 2).expect("legacy tier map should encode");
        for name in ["COLD-Z", "COLD-B"] {
            rmp::encode::write_str(&mut payload, name).expect("legacy tier name should encode");
            let external = to_external_tier_config(name, &candidate.tiers[name]).expect("legacy tier fixture should convert");
            external
                .serialize(&mut rmp_serde::Serializer::new(&mut payload))
                .expect("legacy tier payload should encode");
        }
        let mut persisted = Vec::with_capacity(4 + payload.len());
        persisted.extend_from_slice(&TIER_CONFIG_FORMAT.to_le_bytes());
        persisted.extend_from_slice(&TIER_CONFIG_VERSION.to_le_bytes());
        persisted.extend_from_slice(&payload);
        let persisted_digest = tier_config_digest(&persisted);
        assert_ne!(
            persisted_digest,
            tier_config_candidate_digest(&candidate).expect("current tier config digest should build"),
            "legacy HashMap bytes must exercise the raw-digest compatibility path"
        );
        store
            .insert_config_object(tier_config_path(TIER_CONFIG_FILE), persisted)
            .await;

        let mutation_id = uuid::Uuid::from_u128(28);
        let mut intent = prepared_remove_intent("COLD-A", mutation_id);
        intent.candidate_digest = persisted_digest;
        let canonical_mutation_id = uuid::Uuid::from_u128(29);
        let mut canonical_intent = prepared_remove_intent("COLD-A", canonical_mutation_id);
        canonical_intent.candidate_digest = tier_config_candidate_digest(&candidate).expect("canonical digest should build");
        for prepared in [&intent, &canonical_intent] {
            save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), prepared)
                .await
                .expect("prepared coordinator intent should persist");
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut intents = vec![intent, canonical_intent];
        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::recover_prepared_coordinator_mutation_intents(store.clone(), &mut intents)
                        .await
                        .expect("legacy persisted digest should commit the prepared intent");
                },
            )
            .await;

        assert!(
            intents
                .iter()
                .all(|intent| intent.state == TierMutationIntentState::Committed)
        );
        assert!(
            intents
                .iter()
                .all(|intent| intent.committed_config_etag.as_deref() == Some("reference-proof-etag"))
        );
        assert!(lock_unpoisoned(&calls).is_empty(), "a matching persisted digest must not abort peers");
        let persisted_intents = TierConfigMgr::load_coordinator_mutation_intents(store)
            .await
            .expect("coordinator intent scan should succeed after recovery");
        assert_eq!(persisted_intents.len(), 2);
        assert!(
            persisted_intents
                .iter()
                .all(|intent| intent.state == TierMutationIntentState::Committed)
        );
    }

    #[tokio::test]
    async fn coordinator_recovery_preserves_unexpired_prepared_owner_on_old_config() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("active Prepared recovery fixture should persist");
        let (_, old_config_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("active Prepared old config should load");
        let mutation_id = uuid::Uuid::from_u128(0x2210);
        let mut intent = prepared_remove_intent("COLD-A", mutation_id);
        intent.old_config_etag = old_config_etag;
        intent.expires_at_unix_nanos = tier_mutation_intent_expiry_unix_nanos();
        save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &intent)
            .await
            .expect("active Prepared coordinator intent should persist");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut intents = vec![intent];

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Aborted),
                )],
                async {
                    TierConfigMgr::recover_prepared_coordinator_mutation_intents(store.clone(), &mut intents)
                        .await
                        .expect("active Prepared recovery should retain its owner fence");
                },
            )
            .await;

        assert_eq!(intents[0].state, TierMutationIntentState::Prepared);
        assert!(lock_unpoisoned(&calls).is_empty(), "unexpired owner must not receive recovery Abort");
        let persisted = TierConfigMgr::load_coordinator_mutation_intents(store)
            .await
            .expect("active Prepared coordinator scan should succeed");
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].state, TierMutationIntentState::Prepared);
    }

    #[tokio::test]
    async fn coordinator_mutation_recovery_aborts_expired_unmatched_intent() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (_, old_config_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("old tier config should load with metadata");
        let mutation_id = uuid::Uuid::from_u128(27);
        let mut intent = prepared_remove_intent("COLD-A", mutation_id);
        intent.old_config_etag = old_config_etag;
        save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &intent)
            .await
            .expect("expired unmatched coordinator intent should persist");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let handle = TierConfigMgr::new();

        let err = TIER_MUTATION_TEST_PEERS
            .scope(
                vec![Arc::new(FakeTierMutationPeer {
                    label: "peer-a",
                    calls: calls.clone(),
                    captured_prepares: None,
                    prepare: Ok(PeerTierMutationState::Prepared),
                    prepare_definitely_rejected: false,
                    commit: Ok(PeerTierMutationState::Committed),
                    abort: Err("abort unavailable"),
                }) as Arc<dyn TierMutationPeer>],
                async { TierConfigMgr::reload_handle_with(&handle, store.clone()).await },
            )
            .await
            .expect_err("failed recovery abort must fail closed");
        assert!(
            err.to_string()
                .contains("expired coordinator tier mutation recovery abort failed"),
            "{err}"
        );
        assert!(
            TierConfigMgr::acquire_operation_lease(&handle, "COLD-A").await.is_err(),
            "failed recovery abort must retain the prepared runtime block"
        );
        let intents = TierConfigMgr::load_coordinator_mutation_intents(store.clone())
            .await
            .expect("coordinator intent scan should retain failed abort recovery evidence");
        assert_eq!(intents[0].state, TierMutationIntentState::Prepared);

        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                async {
                    TierConfigMgr::reload_handle_with(&handle, store.clone())
                        .await
                        .expect("expired unmatched coordinator intent should converge by aborting peers");
                },
            )
            .await;

        assert_eq!(
            lock_unpoisoned(&calls).as_slice(),
            &[format!("peer-a:abort:{mutation_id}"), format!("peer-a:abort:{mutation_id}")]
        );
        assert!(handle.read().await.tiers.contains_key("COLD-A"));
        let intents = TierConfigMgr::load_coordinator_mutation_intents(store)
            .await
            .expect("coordinator intent scan should succeed after expired recovery");
        assert!(
            intents.is_empty(),
            "expired unmatched coordinator intent should be removed after abort convergence"
        );
    }

    #[tokio::test]
    async fn coordinator_mutation_recovery_retains_prepared_on_third_config() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("third tier config fixture should persist");

        let mutation_id = uuid::Uuid::from_u128(0x94);
        let mut intent = prepared_remove_intent("COLD-A", mutation_id);
        intent.old_config_etag = Some("different-old-config-etag".to_string());
        intent.candidate_digest = [0x94; 32];
        save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &intent)
            .await
            .expect("prepared coordinator intent should persist");

        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut intents = vec![intent];
        TIER_MUTATION_TEST_PEERS
            .scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    calls.clone(),
                    Ok(PeerTierMutationState::Aborted),
                )],
                async {
                    TierConfigMgr::recover_prepared_coordinator_mutation_intents(store.clone(), &mut intents)
                        .await
                        .expect("uncertain third config should be retained without fanout");
                },
            )
            .await;

        assert_eq!(intents[0].state, TierMutationIntentState::Prepared);
        assert!(lock_unpoisoned(&calls).is_empty(), "uncertain recovery must not contact peers");
        let persisted = TierConfigMgr::load_coordinator_mutation_intents(store)
            .await
            .expect("coordinator scan should retain uncertain evidence");
        assert_eq!(persisted.len(), 1);
        assert_eq!(persisted[0].state, TierMutationIntentState::Prepared);
    }

    #[tokio::test]
    async fn prepared_mutation_recovery_retries_when_runtime_blocks_change() {
        let manager = TierConfigMgr::new();
        let first = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(24));
        let second = prepared_remove_intent("COLD-B", uuid::Uuid::from_u128(25));

        TierConfigMgr::reconcile_prepared_mutation_intents(&manager, std::slice::from_ref(&first))
            .await
            .expect("initial prepared block should reconcile");
        let idempotent_base_revision = TierConfigMgr::prepared_mutation_blocks_revision(&manager).await;
        TierConfigMgr::apply_prepared_mutation_intent_block(&manager, &first)
            .await
            .expect("idempotent peer prepare should refresh the runtime fence");
        assert!(
            TierConfigMgr::reconcile_prepared_mutation_intents_with_revision(
                &manager,
                std::slice::from_ref(&first),
                Some(idempotent_base_revision),
            )
            .await
            .expect("idempotent peer activity must preserve a valid scan")
        );
        let base_revision = TierConfigMgr::prepared_mutation_blocks_revision(&manager).await;
        TierConfigMgr::apply_prepared_mutation_intent_block(&manager, &second)
            .await
            .expect("concurrent peer prepare should update runtime blocks");

        assert!(
            !TierConfigMgr::reconcile_prepared_mutation_intents_with_revision(
                &manager,
                std::slice::from_ref(&first),
                Some(base_revision),
            )
            .await
            .expect("stale revision should be detected without overwriting")
        );
        {
            let guard = manager.read().await;
            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should be registered");
            let blocks = &lock_unpoisoned(&runtime).prepared_mutation_blocks;
            assert_eq!(blocks.get("COLD-A"), Some(&first.mutation_id));
            assert_eq!(blocks.get("COLD-B"), Some(&second.mutation_id));
        }

        let retry_revision = TierConfigMgr::prepared_mutation_blocks_revision(&manager).await;
        assert!(
            TierConfigMgr::reconcile_prepared_mutation_intents_with_revision(
                &manager,
                &[first.clone(), second.clone()],
                Some(retry_revision),
            )
            .await
            .expect("fresh revision should reconcile")
        );
    }

    #[tokio::test]
    async fn mutation_recovery_rescans_both_prefixes_after_runtime_revision_race() {
        use crate::services::tier::tier_mutation_intent::TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX;

        let store = Arc::new(CasConfigStore::default());
        let first = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(0x80));
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &first)
            .await
            .expect("first prepared fixture should persist");
        let barrier = store
            .pause_next_list_with_prefix(TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX)
            .await;
        let manager = TierConfigMgr::new();
        let reload_manager = manager.clone();
        let reload_store = store.clone();
        let recovery = tokio::spawn(async move {
            TierConfigMgr::load_and_reconcile_mutation_recovery_snapshot(&reload_manager, reload_store, true).await
        });

        barrier.arrived.notified().await;
        let second = prepared_remove_intent("COLD-B", uuid::Uuid::from_u128(0x81));
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &second)
            .await
            .expect("racing prepared fixture should persist before its runtime fence");
        TierConfigMgr::apply_prepared_mutation_intent_block(&manager, &second)
            .await
            .expect("racing prepared fixture should update the runtime revision");
        barrier.release.add_permits(1);

        recovery
            .await
            .expect("recovery task should join")
            .expect("stale recovery scan should retry successfully");
        let guard = manager.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
        let runtime = lock_unpoisoned(&runtime);
        assert_eq!(runtime.prepared_mutation_blocks.get("COLD-A"), Some(&first.mutation_id));
        assert_eq!(runtime.prepared_mutation_blocks.get("COLD-B"), Some(&second.mutation_id));
    }

    #[tokio::test]
    async fn reload_restarts_under_config_lock_when_final_scan_finds_coordinator_intent() {
        use crate::services::tier::tier_mutation_intent::TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX;

        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (_, old_config_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config fixture should expose its real ETag");
        let remove_candidate = empty_mgr();
        let mut late_intent = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(0x82));
        late_intent.old_config_etag = old_config_etag;
        late_intent.candidate_digest =
            tier_config_candidate_digest(&remove_candidate).expect("remove candidate digest should be canonical");
        let barrier = store
            .pause_list_with_prefix_after(TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX, 1)
            .await;
        let handle = TierConfigMgr::new();

        TIER_MUTATION_TEST_PEERS
            .scope(Vec::new(), async {
                let reload = TierConfigMgr::reload_handle_with(&handle, store.clone());
                let inject = async {
                    tokio::time::timeout(Duration::from_secs(1), barrier.arrived.notified())
                        .await
                        .expect("reload should reach its final coordinator scan");
                    save_tier_coordinator_mutation_intent_record_if_absent(store.clone(), &late_intent)
                        .await
                        .expect("racing coordinator intent should persist");
                    barrier.release.add_permits(1);
                };
                let (reload_result, ()) = tokio::join!(reload, inject);
                reload_result.expect("reload should restart and recover the late coordinator intent");
            })
            .await;

        assert!(
            store
                .lock_requests
                .lock()
                .expect("tier config lock request log should not poison")
                .iter()
                .any(|(bucket, object)| bucket == RUSTFS_META_BUCKET && object == &tier_config_lock_path()),
            "a coordinator intent found after taking admin_updates must force a namespace-lock restart"
        );
        assert!(
            TierConfigMgr::load_coordinator_mutation_intents(store)
                .await
                .expect("coordinator scan should succeed after recovery")
                .is_empty()
        );
        let guard = handle.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
        assert!(lock_unpoisoned(&runtime).prepared_mutation_blocks.is_empty());
    }

    #[tokio::test]
    async fn reload_handle_fails_closed_when_committed_replay_fails() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");

        let mutation_id = uuid::Uuid::from_u128(16);
        let intent = committed_remove_intent("COLD-A", mutation_id, "etag-new");
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &intent)
            .await
            .expect("committed intent fixture should persist");
        let prepared_id = uuid::Uuid::from_u128(17);
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(
            store.clone(),
            &prepared_remove_intent("COLD-A", prepared_id),
        )
        .await
        .expect("prepared intent fixture should persist");
        let calls = Arc::new(Mutex::new(Vec::new()));
        let handle = TierConfigMgr::new();

        let err = TIER_MUTATION_TEST_PEERS
            .scope(vec![FakeTierMutationPeer::boxed("peer-a", calls.clone(), Err("network down"))], async {
                TierConfigMgr::reload_handle_with(&handle, store.clone()).await
            })
            .await
            .expect_err("reload must fail closed when committed replay fails");

        assert!(err.to_string().contains("network down"), "{err}");
        assert_eq!(lock_unpoisoned(&calls).as_slice(), &[format!("peer-a:commit:{mutation_id}:etag-new")]);
        assert!(
            !handle.read().await.tiers.contains_key("COLD-A"),
            "local manager must not publish persisted config before peer replay succeeds"
        );
        let blocked = match TierConfigMgr::acquire_operation_lease(&handle, "COLD-A").await {
            Ok(_) => panic!("failed committed replay must block old tier operations"),
            Err(err) => err,
        };
        assert!(blocked.message.contains("being replaced"), "{blocked}");
        {
            let guard = handle.read().await;
            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
            let runtime = lock_unpoisoned(&runtime);
            assert_eq!(runtime.prepared_mutation_blocks.get("COLD-A"), Some(&prepared_id));
            assert!(
                runtime
                    .committed_mutation_blocks
                    .get("COLD-A")
                    .is_some_and(|ids| ids.contains(&mutation_id))
            );
        }
        let reloaded = TierConfigMgr::load_tier_mutation_intents(store)
            .await
            .expect("intent scan should succeed after failed replay");
        assert!(
            reloaded.iter().any(|intent| intent.mutation_id == mutation_id),
            "failed committed replay must keep the durable intent for retry"
        );
    }

    #[tokio::test]
    async fn committed_missing_intent_requires_matching_tier_config_etag() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let (_, committed_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("persisted tier config should reload");
        let committed_etag = committed_etag.expect("persisted tier config should carry an ETag");

        assert!(
            tier_config_etag_matches(store.clone(), &committed_etag)
                .await
                .expect("matching ETag proof should load")
        );
        assert!(
            !tier_config_etag_matches(store, "other-etag")
                .await
                .expect("mismatched ETag proof should load"),
            "missing peer intent must not be terminal unless the committed config ETag matches"
        );
    }

    #[tokio::test]
    async fn prepared_mutation_recovery_blocks_new_operation_lease() {
        let manager = TierConfigMgr::new();
        manager
            .write()
            .await
            .tiers
            .insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        let mutation_id = uuid::Uuid::from_u128(1);
        TierConfigMgr::reconcile_prepared_mutation_intents(&manager, &[prepared_remove_intent("COLD-A", mutation_id)])
            .await
            .expect("prepared recovery block should apply");

        let err = match TierConfigMgr::acquire_operation_lease(&manager, "COLD-A").await {
            Ok(_) => panic!("prepared mutation must block new operation leases"),
            Err(err) => err,
        };
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert_eq!(err.message, TIER_MUTATION_BLOCK_MESSAGE);
        let guard = manager.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should be registered by recovery block");
        assert_eq!(lock_unpoisoned(&runtime).prepared_mutation_blocks.get("COLD-A"), Some(&mutation_id));
    }

    #[tokio::test]
    async fn prepared_mutation_recovery_blocks_admin_publish() {
        let manager = TierConfigMgr::new();
        manager
            .write()
            .await
            .tiers
            .insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        TierConfigMgr::reconcile_prepared_mutation_intents(
            &manager,
            &[prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(1))],
        )
        .await
        .expect("prepared recovery block should apply");
        let candidate = empty_mgr();

        let err = TierConfigMgr::publish_candidate(&manager, candidate, None)
            .await
            .expect_err("prepared mutation must block conflicting admin publishes");
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert!(manager.read().await.tiers.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn publish_allowance_rejects_revision_change_before_transition() {
        let manager = TierConfigMgr::new();
        manager
            .write()
            .await
            .tiers
            .insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        let intent = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(0x8f));
        TierConfigMgr::apply_prepared_mutation_intent_block(&manager, &intent)
            .await
            .expect("prepared intent should install a fence");
        let allowance = MutationBlockAllowance {
            mutation_ids: HashSet::from([intent.mutation_id]),
            revision: TierConfigMgr::mutation_blocks_revision(&manager).await,
        };
        TierConfigMgr::clear_prepared_mutation_intent_block(&manager, intent.mutation_id)
            .await
            .expect("clearing the replaced prepared mutation should advance the runtime revision");
        let concurrent_intent = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(0x8e));
        TierConfigMgr::apply_prepared_mutation_intent_block(&manager, &concurrent_intent)
            .await
            .expect("a concurrent prepared mutation should advance the runtime revision");

        let update = TierConfigMgr::admin_update_lock(&manager).await;
        let err = TierConfigMgr::publish_candidate_owned_with_allowed_mutation_blocks(
            &manager,
            empty_mgr(),
            None,
            update,
            Some(allowance),
        )
        .await
        .expect_err("a stale allowance must fail before the transition starts");
        assert!(err.message.contains("changed before replacement"), "{err}");
        assert!(manager.read().await.tiers.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn publish_allowance_rechecks_revision_after_active_leases_drain() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old generation lease should be available");
        let intent = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(0x90));
        TierConfigMgr::apply_prepared_mutation_intent_block(&manager, &intent)
            .await
            .expect("prepared intent should install a fence");
        let allowance = MutationBlockAllowance {
            mutation_ids: HashSet::from([intent.mutation_id]),
            revision: TierConfigMgr::mutation_blocks_revision(&manager).await,
        };
        let update = TierConfigMgr::admin_update_lock(&manager).await;

        let publish = TierConfigMgr::publish_candidate_owned_with_allowed_mutation_blocks(
            &manager,
            empty_mgr(),
            None,
            update,
            Some(allowance),
        );
        let race = async {
            tokio::time::timeout(Duration::from_secs(1), async {
                while old.inner.accepting.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("publish should revoke the old generation before waiting");
            TierConfigMgr::apply_prepared_mutation_intent_block(&manager, &intent)
                .await
                .expect("idempotent prepare should preserve the allowance during drain");
            drop(old);
        };
        let (publish_result, ()) = tokio::join!(publish, race);
        publish_result.expect("an idempotent prepare must not invalidate the publish allowance");
        assert!(!manager.read().await.tiers.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn publish_rechecks_mutation_blocks_after_active_leases_drain() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old generation lease should be available");
        let publish_manager = manager.clone();
        let publish = tokio::spawn(async move { TierConfigMgr::publish_candidate(&publish_manager, empty_mgr(), None).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while old.inner.accepting.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("publish should revoke the old generation before waiting");

        let intent = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(0x91));
        TierConfigMgr::apply_prepared_mutation_intent_block(&manager, &intent)
            .await
            .expect("racing prepared intent should install a fence");
        drop(old);
        let err = publish
            .await
            .expect("publish task should join")
            .expect_err("publish must reject a fence that arrived during drain");
        assert!(err.message.contains("changed before publish"), "{err}");
        assert!(manager.read().await.tiers.contains_key("COLD-A"));
        let blocked = match TierConfigMgr::acquire_operation_lease(&manager, "COLD-A").await {
            Ok(_) => panic!("racing prepared intent must remain fenced after rollback"),
            Err(err) => err,
        };
        assert_eq!(blocked.message, TIER_MUTATION_BLOCK_MESSAGE);
    }

    #[tokio::test]
    async fn prepared_mutation_recovery_is_idempotent_and_rejects_conflicts() {
        let manager = TierConfigMgr::new();
        let first = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(1));
        TierConfigMgr::reconcile_prepared_mutation_intents(&manager, &[first.clone(), first])
            .await
            .expect("same prepared mutation should be idempotent");

        let first = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(1));
        let second = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(2));
        let err = TierConfigMgr::reconcile_prepared_mutation_intents(&manager, &[first, second])
            .await
            .expect_err("a second prepared mutation for the same tier must fail closed");
        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
    }

    #[tokio::test]
    async fn prepared_mutation_recovery_clears_resolved_intents() {
        let manager = TierConfigMgr::new();
        manager
            .write()
            .await
            .tiers
            .insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        TierConfigMgr::reconcile_prepared_mutation_intents(
            &manager,
            &[prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(1))],
        )
        .await
        .expect("prepared recovery block should apply");
        TierConfigMgr::reconcile_prepared_mutation_intents(&manager, &[])
            .await
            .expect("resolved recovery scan should clear stale blocks");

        let guard = manager.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should stay registered");
        assert!(lock_unpoisoned(&runtime).prepared_mutation_blocks.is_empty());
    }

    #[tokio::test]
    async fn reload_handle_clears_resolved_prepared_intent_from_store_scan() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");

        let mutation_id = uuid::Uuid::from_u128(23);
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(
            store.clone(),
            &prepared_remove_intent("COLD-A", mutation_id),
        )
        .await
        .expect("prepared intent fixture should persist");
        let handle = TierConfigMgr::new();

        TierConfigMgr::reload_handle_with(&handle, store.clone())
            .await
            .expect("reload should restore prepared intent from store");
        let blocked = match TierConfigMgr::acquire_operation_lease(&handle, "COLD-A").await {
            Ok(_) => panic!("prepared intent restored from store must block new operation leases"),
            Err(err) => err,
        };
        assert_eq!(blocked.message, TIER_MUTATION_BLOCK_MESSAGE);

        crate::services::tier::tier_mutation_intent::delete_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("prepared intent fixture should delete");
        TierConfigMgr::reload_handle_with(&handle, store)
            .await
            .expect("reload should clear resolved prepared intent blocks");
        let guard = handle.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should stay registered");
        assert!(lock_unpoisoned(&runtime).prepared_mutation_blocks.is_empty());
    }

    #[tokio::test]
    async fn registry_steady_state_lookup_does_not_scan_misses() {
        let manager = TierConfigMgr::new();
        TIER_REGISTRY_MISS_SCAN_COUNT
            .scope(std::cell::Cell::new(0), async {
                let guard = manager.read().await;
                let first = tier_driver_runtime(&manager, &guard);
                assert_eq!(TIER_REGISTRY_MISS_SCAN_COUNT.with(std::cell::Cell::get), 1);
                let second = tier_driver_runtime(&manager, &guard);
                assert!(Arc::ptr_eq(&first, &second));
                assert_eq!(TIER_REGISTRY_MISS_SCAN_COUNT.with(std::cell::Cell::get), 1);
            })
            .await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn cold_driver_build_does_not_hold_manager_write_lock() {
        let cold_tier = "COLD-BUILD-LOCK";
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            guard.tiers.insert(cold_tier.to_string(), build_rustfs_tier(cold_tier));
            install_lease_backend(&mut guard, "COLD-B", LeaseTestBackend::ready("b"));
        }
        let (barrier, _barrier_guard) = install_tier_driver_build_barrier(cold_tier);
        let build_manager = manager.clone();
        let build = tokio::spawn(async move { TierConfigMgr::acquire_operation_lease(&build_manager, cold_tier).await });
        barrier.arrived.notified().await;

        drop(
            tokio::time::timeout(Duration::from_millis(100), manager.read())
                .await
                .expect("cold driver construction must not block manager readers"),
        );
        let tier_b = tokio::time::timeout(Duration::from_millis(100), TierConfigMgr::acquire_operation_lease(&manager, "COLD-B"))
            .await
            .expect("cold tier A construction must not block tier B")
            .expect("tier B lease should remain available");
        drop(tier_b);

        barrier.release.add_permits(1);
        build
            .await
            .expect("cold driver build task should join")
            .expect("cold driver should finish after the test barrier releases");
    }

    #[tokio::test(start_paused = true)]
    #[serial_test::serial]
    async fn cold_reload_driver_build_timeout_restores_config() {
        let cold_tier = "COLD-BUILD-TIMEOUT";
        let manager = TierConfigMgr::new();
        manager
            .write()
            .await
            .tiers
            .insert(cold_tier.to_string(), build_rustfs_tier(cold_tier));
        let mut candidate = empty_mgr();
        let mut replacement = build_rustfs_tier(cold_tier);
        replacement.rustfs.as_mut().expect("replacement payload should exist").prefix = "new-prefix".to_string();
        candidate.tiers.insert(cold_tier.to_string(), replacement);
        let (_barrier, _barrier_guard) = install_tier_driver_build_barrier(cold_tier);

        let err = TierConfigMgr::publish_candidate(&manager, candidate, None)
            .await
            .expect_err("stalled cold backend construction must time out");

        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(manager.read().await.tiers.contains_key(cold_tier));
        let guard = manager.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
        assert!(lock_unpoisoned(&runtime).draining.is_empty());
    }

    #[test]
    fn internal_tier_snapshot_preserves_credentials() {
        let mut config = build_rustfs_tier("COLD-A");
        let rustfs = config.rustfs.as_mut().expect("rustfs payload should exist");
        rustfs.access_key = "access-key".to_string();
        rustfs.secret_key = "secret-key".to_string();

        let snapshot = config.clone_with_credentials();

        let rustfs = snapshot.rustfs.expect("snapshot payload should exist");
        assert_eq!(rustfs.access_key, "access-key");
        assert_eq!(rustfs.secret_key, "secret-key");
    }

    #[tokio::test(start_paused = true)]
    async fn remote_mutation_validation_timeout_restores_candidate() {
        let mut candidate = empty_mgr();
        candidate.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::pending_in_use("pending")));

        let err = apply_tier_candidate_mutation(
            TierCandidateMutation::Remove("COLD-A".to_string(), false),
            &mut candidate,
            Instant::now() + TIER_REMOTE_VALIDATION_TIMEOUT,
        )
        .await
        .expect_err("unbounded backend validation must time out");

        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(candidate.tiers.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn cold_cached_reload_rejects_in_use_route_change_and_removal() {
        for remove in [false, true] {
            let manager = TierConfigMgr::new();
            {
                let mut guard = manager.write().await;
                guard.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
                guard
                    .driver_cache
                    .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::in_use("cold")));
            }
            let mut candidate = empty_mgr();
            if !remove {
                let mut replacement = build_rustfs_tier("COLD-A");
                replacement.rustfs.as_mut().expect("replacement payload should exist").prefix = "new-prefix".to_string();
                candidate.tiers.insert("COLD-A".to_string(), replacement);
            }

            let err = TierConfigMgr::publish_candidate(&manager, candidate, None)
                .await
                .expect_err("an in-use cold-cache destination must not be rebound or removed");

            assert_eq!(err.code, ERR_TIER_BACKEND_NOT_EMPTY.code);
            assert!(manager.read().await.tiers.contains_key("COLD-A"));
            let restored = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
                .await
                .expect("failed reload must restore the old cold-cache generation");
            assert!(restored.is_current(&manager).await);
        }
    }

    #[tokio::test]
    async fn empty_backend_route_change_and_credential_rotation_can_publish() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("empty"));
        }
        let mut route_candidate = empty_mgr();
        let mut replacement = build_rustfs_tier("COLD-A");
        replacement.rustfs.as_mut().expect("replacement payload should exist").prefix = "new-prefix".to_string();
        route_candidate.tiers.insert("COLD-A".to_string(), replacement);
        TierConfigMgr::publish_candidate(&manager, route_candidate, None)
            .await
            .expect("an empty backend may be rebound");

        {
            let mut guard = manager.write().await;
            guard
                .driver_cache
                .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::panicking_in_use("credentials")));
        }
        let mut credential_candidate = empty_mgr();
        let mut rotated = manager.read().await.tiers["COLD-A"].clone_with_credentials();
        rotated.rustfs.as_mut().expect("rotated payload should exist").secret_key = "rotated".to_string();
        credential_candidate.tiers.insert("COLD-A".to_string(), rotated);
        TierConfigMgr::publish_candidate(&manager, credential_candidate, None)
            .await
            .expect("credential-only rotation must not inspect backend occupancy");
    }

    #[tokio::test]
    async fn slow_tier_operation_does_not_block_another_tier() {
        let slow = LeaseTestBackend::blocking("slow");
        let fast = LeaseTestBackend::ready("fast");
        let manager = Arc::new(RwLock::new(empty_mgr()));
        {
            let mut manager = manager.write().await;
            install_lease_backend(&mut manager, "COLD-A", slow.clone());
            install_lease_backend(&mut manager, "COLD-B", fast.clone());
        }
        let slow_lease = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("slow lease should be available");
        let fast_lease = TierConfigMgr::acquire_operation_lease(&manager, "COLD-B")
            .await
            .expect("fast lease should be available");
        let slow_task = tokio::spawn(async move { slow_lease.remove("object", "").await });
        slow.remove_started
            .as_ref()
            .expect("slow backend should expose start notification")
            .notified()
            .await;

        let manager_read = tokio::time::timeout(Duration::from_millis(100), manager.read())
            .await
            .expect("manager reads must not wait for tier A");
        assert!(manager_read.is_tier_valid("COLD-B"));
        drop(manager_read);

        tokio::time::timeout(Duration::from_millis(100), fast_lease.remove("object", ""))
            .await
            .expect("tier B must not wait for tier A")
            .expect("fast remove should succeed");
        assert_eq!(fast.calls(), vec!["fast"]);

        slow.remove_release
            .as_ref()
            .expect("slow backend should expose release semaphore")
            .add_permits(1);
        slow_task
            .await
            .expect("slow task should join")
            .expect("slow remove should succeed");
    }

    #[tokio::test]
    async fn slow_admin_verify_does_not_hold_manager_lock() {
        let manager = TierConfigMgr::new();
        let backend = LeaseTestBackend::blocking("verify");
        let started = backend
            .remove_started
            .as_ref()
            .expect("verify remove notifier should exist")
            .clone();
        let release = backend
            .remove_release
            .as_ref()
            .expect("verify remove release should exist")
            .clone();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", backend);
        }

        let verify_manager = manager.clone();
        let verify = tokio::spawn(async move { TierConfigMgr::verify_without_manager_lock(&verify_manager, "COLD-A").await });
        started.notified().await;
        drop(
            tokio::time::timeout(Duration::from_millis(100), manager.read())
                .await
                .expect("slow verify must not hold the manager lock"),
        );
        release.add_permits(1);
        verify.await.expect("verify task should join").expect("verify should finish");
    }

    #[tokio::test]
    async fn cancelled_admin_verify_finishes_probe_cleanup_and_releases_its_lease() {
        let manager = TierConfigMgr::new();
        let backend = RecordingWarmBackend::new();
        let get_barrier = backend.arm_get_barrier().await;
        {
            let mut guard = manager.write().await;
            guard.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
            guard
                .replace_driver("COLD-A", Box::new(backend.clone()))
                .expect("verification backend generation should install");
        }

        let verify_manager = manager.clone();
        let verify = tokio::spawn(async move { TierConfigMgr::verify_without_manager_lock(&verify_manager, "COLD-A").await });
        get_barrier.wait_until_paused().await;
        verify.abort();
        assert!(
            verify
                .await
                .expect_err("the outer verify caller should be cancelled")
                .is_cancelled()
        );
        assert_eq!(TierConfigMgr::active_operation_lease_count(&manager, "COLD-A").await, 1);

        get_barrier.release();
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if backend.object_count().await == 0 && TierConfigMgr::active_operation_lease_count(&manager, "COLD-A").await == 0
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the detached verification must finish cleanup and release its lease");

        let operations = backend.op_log().await;
        assert!(matches!(
            operations.as_slice(),
            [
                MockWarmOp::Put { .. },
                MockWarmOp::Probe { .. },
                MockWarmOp::Get { .. },
                MockWarmOp::Remove { .. },
                MockWarmOp::Probe { .. }
            ]
        ));
    }

    #[tokio::test]
    async fn same_name_replacement_drains_old_generation_and_routes_new_work_to_new_driver() {
        let old = LeaseTestBackend::ready("old");
        let new = LeaseTestBackend::ready("new");
        let manager = Arc::new(RwLock::new(empty_mgr()));
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", old.clone());
        }
        let old_lease = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old generation lease should be available");
        {
            manager
                .write()
                .await
                .replace_driver("COLD-A", Box::new(new.clone()))
                .expect("replacement generation should install");
        }
        let new_lease = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("new generation lease should be available");

        assert!(!old_lease.is_current(&manager).await);
        assert!(new_lease.is_current(&manager).await);
        assert!(new_lease.generation() > old_lease.generation());
        old_lease
            .remove("old-object", "")
            .await
            .expect("draining lease should remain usable");
        new_lease
            .remove("new-object", "")
            .await
            .expect("new lease should use replacement driver");
        assert_eq!(old.calls(), vec!["old"]);
        assert_eq!(new.calls(), vec!["new"]);
    }

    #[tokio::test]
    async fn replacement_waits_for_inflight_operation_lease() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old lease should be available");
        let mut candidate = empty_mgr();
        let mut config = build_rustfs_tier("COLD-A");
        config.rustfs.as_mut().expect("rustfs payload should exist").endpoint = "https://replacement.invalid".to_string();
        candidate.tiers.insert("COLD-A".to_string(), config);
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("new")));
        let publish_manager = manager.clone();
        let publish =
            tokio::spawn(async move { TierConfigMgr::publish_candidate(&publish_manager, candidate, Some("COLD-A")).await });

        tokio::time::timeout(Duration::from_secs(1), async {
            while old.is_current(&manager).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement should revoke the old generation before waiting");
        assert!(!publish.is_finished(), "replacement must wait for the old operation lease");
        drop(old);
        publish
            .await
            .expect("publish task should join")
            .expect("replacement should publish after the lease drains");
    }

    #[tokio::test(start_paused = true)]
    async fn replacement_drain_timeout_restores_generation_and_allows_retry() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old lease should be available");
        let mut candidate = empty_mgr();
        let mut config = build_rustfs_tier("COLD-A");
        config.rustfs.as_mut().expect("rustfs payload should exist").endpoint = "https://timeout.invalid".to_string();
        candidate.tiers.insert("COLD-A".to_string(), config);
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("timed-out")));
        let publish_manager = manager.clone();
        let publish =
            tokio::spawn(async move { TierConfigMgr::publish_candidate(&publish_manager, candidate, Some("COLD-A")).await });
        while old.inner.accepting.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        tokio::time::advance(TIER_OPERATION_DRAIN_TIMEOUT).await;
        let err = publish
            .await
            .expect("publish task should join")
            .expect_err("replacement must time out while the old lease remains active");
        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(old.is_current(&manager).await, "timeout rollback must restore the old generation");
        {
            let guard = manager.read().await;
            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
            assert!(!lock_unpoisoned(&runtime).draining.contains_key("COLD-A"));
        }
        drop(old);

        let mut retry = empty_mgr();
        let mut retry_config = build_rustfs_tier("COLD-A");
        retry_config.rustfs.as_mut().expect("rustfs payload should exist").endpoint = "https://retry.invalid".to_string();
        retry.tiers.insert("COLD-A".to_string(), retry_config);
        retry
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("retry")));
        TierConfigMgr::publish_candidate(&manager, retry, Some("COLD-A"))
            .await
            .expect("a later replacement must succeed after timeout rollback");
    }

    #[tokio::test]
    async fn generation_prepare_failure_restores_old_manager_and_generation() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let lease = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old generation should initialize");
        drop(lease);
        {
            let guard = manager.read().await;
            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should be registered");
            lock_unpoisoned(&runtime).next_generation = u64::MAX;
        }
        let mut candidate = empty_mgr();
        let mut config = build_rustfs_tier("COLD-A");
        config.rustfs.as_mut().expect("rustfs payload should exist").endpoint = "https://overflow.invalid".to_string();
        candidate.tiers.insert("COLD-A".to_string(), config);
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("new")));

        let err = TierConfigMgr::publish_candidate(&manager, candidate, Some("COLD-A"))
            .await
            .expect_err("generation exhaustion must fail before manager state changes");
        assert!(err.message.contains("generation exhausted"));
        assert_eq!(
            manager.read().await.tiers["COLD-A"]
                .rustfs
                .as_ref()
                .expect("old config should remain")
                .endpoint,
            "https://example-compat.invalid"
        );
        let restored = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("prepare failure must restore the old generation");
        assert!(restored.is_current(&manager).await);
        let guard = manager.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
        assert!(!lock_unpoisoned(&runtime).draining.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn legacy_reload_does_not_block_revoked_operation_completion() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old lease should be available");
        let mut candidate = empty_mgr();
        let mut config = build_rustfs_tier("COLD-A");
        config.rustfs.as_mut().expect("rustfs payload should exist").endpoint = "https://legacy-reload.invalid".to_string();
        candidate.tiers.insert("COLD-A".to_string(), config);
        let reload_manager = manager.clone();
        let reload = tokio::spawn(async move { reload_manager.write().await.publish_legacy_reload(candidate).await });
        while old.inner.accepting.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        assert!(
            manager.try_read().is_err(),
            "legacy reload must still hold the manager write guard while draining"
        );
        assert!(!reload.is_finished(), "legacy reload must wait for the active generation");
        let operation = tokio::spawn(async move {
            assert!(!old.is_current_generation(), "the revoked operation must observe the generation fence");
            drop(old);
        });
        tokio::time::timeout(Duration::from_secs(1), operation)
            .await
            .expect("the revoked operation must finish without waiting for the manager guard")
            .expect("the revoked operation task should join");
        reload
            .await
            .expect("legacy reload task should join")
            .expect("legacy reload should publish after the lease drains");
        assert_eq!(
            manager.read().await.tiers["COLD-A"]
                .rustfs
                .as_ref()
                .expect("reloaded tier should exist")
                .endpoint,
            "https://legacy-reload.invalid"
        );
    }

    #[tokio::test]
    async fn reload_publish_waits_for_inflight_operation_lease() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old lease should be available");
        let mut replacement = build_rustfs_tier("COLD-A");
        replacement.rustfs.as_mut().expect("rustfs payload should exist").endpoint = "https://reload.invalid".to_string();
        let mut candidate = empty_mgr();
        candidate.tiers.insert("COLD-A".to_string(), replacement);
        let reload_manager = manager.clone();
        let reload = tokio::spawn(async move { TierConfigMgr::publish_candidate(&reload_manager, candidate, None).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while old.is_current(&manager).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("reload publish should revoke before waiting");
        assert!(!reload.is_finished(), "reload must wait for the old operation lease");
        drop(old);
        reload
            .await
            .expect("reload task should join")
            .expect("reload should publish after the lease drains");
    }

    #[tokio::test]
    async fn cancelled_publish_caller_does_not_leave_tier_draining() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old lease should be available");
        let mut candidate = empty_mgr();
        let mut config = build_rustfs_tier("COLD-A");
        config.rustfs.as_mut().expect("rustfs payload should exist").endpoint = "https://owned.invalid".to_string();
        candidate.tiers.insert("COLD-A".to_string(), config);
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("new")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;
        let publish_manager = manager.clone();
        let caller = tokio::spawn(async move {
            TierConfigMgr::publish_candidate_owned(&publish_manager, candidate, Some("COLD-A".to_string()), update).await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while old.is_current(&manager).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned publish should revoke before caller cancellation");
        caller.abort();

        let mut second = empty_mgr();
        let mut second_config = build_rustfs_tier("COLD-A");
        second_config.rustfs.as_mut().expect("rustfs payload should exist").endpoint = "https://second.invalid".to_string();
        second.tiers.insert("COLD-A".to_string(), second_config);
        second
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("second")));
        let second_manager = manager.clone();
        let second_publish =
            tokio::spawn(async move { TierConfigMgr::publish_candidate(&second_manager, second, Some("COLD-A")).await });
        tokio::task::yield_now().await;
        assert!(!second_publish.is_finished(), "a later update must remain behind the detached publish");

        drop(old);
        second_publish
            .await
            .expect("second publish task should join")
            .expect("second publish should follow the detached publish");
        let endpoint = manager
            .read()
            .await
            .tiers
            .get("COLD-A")
            .and_then(|tier| tier.rustfs.as_ref())
            .expect("second tier should be published")
            .endpoint
            .clone();
        assert_eq!(endpoint, "https://second.invalid");
    }

    #[tokio::test]
    async fn slow_tier_replacement_does_not_block_other_tiers_or_manager_reads() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("a"));
            install_lease_backend(&mut guard, "COLD-B", LeaseTestBackend::ready("b"));
        }
        let old_a = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("tier A lease should be available");
        let old_b = TierConfigMgr::acquire_operation_lease(&manager, "COLD-B")
            .await
            .expect("tier B lease should be available");
        let mut candidate = empty_mgr();
        candidate.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        candidate.tiers.insert("COLD-B".to_string(), build_rustfs_tier("COLD-B"));
        candidate
            .tiers
            .get_mut("COLD-A")
            .and_then(|tier| tier.rustfs.as_mut())
            .expect("tier A payload should exist")
            .endpoint = "https://replacement.invalid".to_string();
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("new-a")));
        let publish_manager = manager.clone();
        let publish =
            tokio::spawn(async move { TierConfigMgr::publish_candidate(&publish_manager, candidate, Some("COLD-A")).await });

        tokio::time::timeout(Duration::from_secs(1), async {
            while old_a.is_current(&manager).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("tier A should enter draining");
        {
            let guard = manager.read().await;
            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
            assert_eq!(
                lock_unpoisoned(&runtime).draining.keys().cloned().collect::<Vec<_>>(),
                vec!["COLD-A".to_string()]
            );
        }
        drop(
            tokio::time::timeout(Duration::from_secs(1), manager.read())
                .await
                .expect("manager reads must not wait for tier A leases"),
        );
        let next_b = tokio::time::timeout(Duration::from_secs(1), TierConfigMgr::acquire_operation_lease(&manager, "COLD-B"))
            .await
            .expect("tier B lease acquisition must not wait for tier A")
            .expect("tier B lease should remain available");
        assert_eq!(next_b.generation(), old_b.generation());
        assert!(!publish.is_finished(), "tier A replacement should still wait for its lease");

        drop(old_a);
        publish
            .await
            .expect("publish task should join")
            .expect("tier A replacement should publish after its lease drains");
    }

    #[tokio::test]
    async fn direct_mutation_fails_closed_while_generation_is_active() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let lease = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("lease should be available");
        let err = manager
            .write()
            .await
            .remove("COLD-A", true)
            .await
            .expect_err("direct removal must not wait for an active generation");
        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(lease.is_current(&manager).await);
    }

    #[tokio::test]
    async fn steady_lease_acquisition_is_concurrent_across_tiers() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("a"));
            install_lease_backend(&mut guard, "COLD-B", LeaseTestBackend::ready("b"));
        }
        let mut tasks = Vec::new();
        for index in 0..200 {
            let manager = manager.clone();
            tasks.push(tokio::spawn(async move {
                let tier = if index % 2 == 0 { "COLD-A" } else { "COLD-B" };
                TierConfigMgr::acquire_operation_lease(&manager, tier).await
            }));
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            for task in tasks {
                task.await
                    .expect("lease task should join")
                    .expect("published generation should remain available");
            }
        })
        .await
        .expect("steady lease acquisition should not serialize on manager writes or config hashing");
    }

    #[tokio::test]
    async fn cancelled_operation_releases_generation_lease() {
        let backend = LeaseTestBackend::blocking("cancelled");
        let manager = Arc::new(RwLock::new(empty_mgr()));
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", backend.clone());
        }
        let lease = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("lease should be available");
        let task = tokio::spawn(async move { lease.remove("object", "").await });
        backend
            .remove_started
            .as_ref()
            .expect("blocking backend should expose start notification")
            .notified()
            .await;
        task.abort();
        let _ = task.await;

        let manager = manager.read().await;
        let runtime = registered_tier_driver_runtime(&manager).expect("runtime sidecar should be registered");
        drop(manager);
        let runtime = lock_unpoisoned(&runtime);
        let generation = runtime.generations.get("COLD-A").expect("generation should remain installed");
        assert_eq!(generation.active_leases.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn tier_generations_are_isolated_between_runtime_instances() {
        let manager_a = TierConfigMgr::new();
        let manager_b = TierConfigMgr::new();
        {
            let mut guard = manager_a.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("a"));
        }
        {
            let mut guard = manager_b.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("b"));
        }
        let lease_a = TierConfigMgr::acquire_operation_lease(&manager_a, "COLD-A")
            .await
            .expect("instance A lease should be available");
        let lease_b = TierConfigMgr::acquire_operation_lease(&manager_b, "COLD-A")
            .await
            .expect("instance B lease should be available");

        assert!(lease_a.is_current(&manager_a).await);
        assert!(lease_b.is_current(&manager_b).await);
        assert!(!lease_a.is_current(&manager_b).await);
        assert!(!lease_b.is_current(&manager_a).await);
    }

    #[tokio::test]
    async fn lease_acquisition_has_constant_generation_state() {
        let manager = Arc::new(RwLock::new(empty_mgr()));
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("bounded"));
        }
        for _ in 0..10_000 {
            drop(
                TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
                    .await
                    .expect("lease should be available"),
            );
        }

        let manager = manager.read().await;
        let runtime = registered_tier_driver_runtime(&manager).expect("runtime sidecar should be registered");
        drop(manager);
        let runtime = lock_unpoisoned(&runtime);
        assert_eq!(runtime.generations.len(), 1);
        assert_eq!(runtime.next_generation, 1);
        assert_eq!(
            runtime
                .generations
                .get("COLD-A")
                .expect("generation should remain installed")
                .active_leases
                .load(Ordering::Acquire),
            0
        );
    }

    #[tokio::test]
    async fn unrelated_reload_keeps_active_generation_current() {
        let manager = Arc::new(RwLock::new(empty_mgr()));
        {
            let mut manager = manager.write().await;
            install_lease_backend(&mut manager, "COLD-A", LeaseTestBackend::ready("a"));
            manager.tiers.insert("COLD-B".to_string(), build_rustfs_tier("COLD-B"));
        }
        let lease = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("lease should be available");
        let generation = lease.generation();
        let mut reloaded = HashMap::from([
            ("COLD-A".to_string(), build_rustfs_tier("COLD-A")),
            ("COLD-B".to_string(), build_rustfs_tier("COLD-B")),
            ("COLD-C".to_string(), build_rustfs_tier("COLD-C")),
        ]);
        reloaded
            .get_mut("COLD-B")
            .and_then(|tier| tier.rustfs.as_mut())
            .expect("tier B payload should exist")
            .access_key = "rotated".to_string();
        manager
            .write()
            .await
            .apply_reloaded_tiers(reloaded)
            .expect("unrelated reload should apply");

        assert!(lease.is_current(&manager).await);
        let next = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("tier A should remain available");
        assert_eq!(next.generation(), generation);
    }

    #[tokio::test]
    async fn backend_identity_survives_credentials_rotation_but_rejects_route_change() {
        let manager = Arc::new(RwLock::new(empty_mgr()));
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old lease should be available");
        let identity = old.backend_identity();

        let mut rotated = build_rustfs_tier("COLD-A");
        let rotated_config = rotated.rustfs.as_mut().expect("rustfs payload should exist");
        rotated_config.access_key = "rotated-access".to_string();
        rotated_config.secret_key = "rotated-secret".to_string();
        manager.write().await.tiers.insert("COLD-A".to_string(), rotated);
        manager
            .write()
            .await
            .replace_driver("COLD-A", Box::new(LeaseTestBackend::ready("rotated")))
            .expect("rotated driver should install");
        let rotated = TierConfigMgr::acquire_operation_lease_for_backend_identity(&manager, "COLD-A", identity)
            .await
            .expect("credential rotation should preserve backend identity");
        assert_ne!(rotated.generation(), old.generation());
        assert_eq!(rotated.backend_identity(), identity);

        let mut moved = build_rustfs_tier("COLD-A");
        moved.rustfs.as_mut().expect("rustfs payload should exist").endpoint = "https://moved.invalid".to_string();
        manager.write().await.tiers.insert("COLD-A".to_string(), moved);
        manager
            .write()
            .await
            .replace_driver("COLD-A", Box::new(LeaseTestBackend::ready("moved")))
            .expect("moved driver should install");
        let err = match TierConfigMgr::acquire_operation_lease_for_backend_identity(&manager, "COLD-A", identity).await {
            Ok(_) => panic!("route change must fail closed"),
            Err(err) => err,
        };
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
    }

    fn build_azure_tier(account_name: &str) -> TierConfig {
        TierConfig {
            version: "v1".to_string(),
            tier_type: TierType::Azure,
            name: "COLD-AZURE".to_string(),
            azure: Some(crate::services::tier::tier_config::TierAzure {
                endpoint: "https://blob.example.invalid".to_string(),
                access_key: account_name.to_string(),
                secret_key: "account-key".to_string(),
                bucket: "archive".to_string(),
                prefix: "objects/".to_string(),
                region: "us-east-1".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn operation_lease_rejects_nonempty_versions_without_exact_get_delete() {
        let manager = TierConfigMgr::new();
        let azure_tier = build_azure_tier("account-a");
        let azure_tier_name = azure_tier.name.clone();
        let exact_tier = build_rustfs_tier("COLD-EXACT");
        let exact_tier_name = exact_tier.name.clone();
        {
            let mut guard = manager.write().await;
            guard.tiers.insert(azure_tier_name.clone(), azure_tier);
            guard
                .replace_driver(&azure_tier_name, Box::new(LeaseTestBackend::ready("azure")))
                .expect("test Azure tier driver should install");
            guard.tiers.insert(exact_tier_name.clone(), exact_tier);
            guard
                .replace_driver(&exact_tier_name, Box::new(LeaseTestBackend::ready("exact")))
                .expect("test exact tier driver should install");
        }

        let lease = TierConfigMgr::acquire_operation_lease(&manager, &azure_tier_name)
            .await
            .expect("tier operation lease should resolve");
        let err = lease
            .validate_remote_version_id("provider-version")
            .expect_err("a provider without exact GET and DELETE must not commit a versioned transition");

        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        lease
            .validate_remote_version_id("")
            .expect("unversioned remote state remains supported");

        TierConfigMgr::acquire_operation_lease(&manager, &exact_tier_name)
            .await
            .expect("exact tier operation lease should resolve")
            .validate_remote_version_id("provider-version")
            .expect("providers declaring exact GET and DELETE must retain versioned transition support");
    }

    #[test]
    fn azure_account_name_is_part_of_backend_identity() {
        let account_a = build_azure_tier("account-a");
        let mut rotated_key = build_azure_tier("account-a");
        rotated_key.azure.as_mut().expect("azure payload should exist").secret_key = "rotated-key".to_string();
        let account_b = build_azure_tier("account-b");

        assert_eq!(
            tier_backend_identity(&account_a).expect("account A identity should encode"),
            tier_backend_identity(&rotated_key).expect("rotated key identity should encode"),
            "Azure account-key rotation must preserve destination identity"
        );
        assert_ne!(
            tier_backend_identity(&account_a).expect("account A identity should encode"),
            tier_backend_identity(&account_b).expect("account B identity should encode"),
            "Azure account-name changes must fence cleanup from the new account"
        );
    }

    #[test]
    fn wasabi_backend_identity_uses_its_canonical_physical_destination() {
        let mut wasabi = build_wasabi_tier("COLD-WASABI");
        let wasabi_config = wasabi.wasabi.as_mut().expect("Wasabi payload should exist");
        wasabi_config.endpoint = "https://s3.nl-1.wasabisys.com".to_string();
        wasabi_config.prefix = "/archive//".to_string();
        wasabi_config.region = "eu-central-1".to_string();

        let mut physical_s3 = build_s3_tier("COLD-WASABI");
        let s3 = physical_s3.s3.as_mut().expect("S3 payload should exist");
        s3.endpoint = "https://s3.eu-central-1.wasabisys.com".to_string();
        s3.bucket = wasabi_config.bucket.clone();
        s3.prefix = "archive".to_string();
        s3.region = wasabi_config.region.clone();

        assert_eq!(
            tier_backend_identity(&wasabi).expect("Wasabi identity should encode"),
            tier_backend_identity(&physical_s3).expect("physical S3 identity should encode")
        );
    }

    #[test]
    fn in_place_wasabi_type_changes_are_rejected() {
        let mut wasabi = build_wasabi_tier("COLD-WASABI");
        wasabi.wasabi.as_mut().expect("Wasabi payload should exist").region = "us-east-1".to_string();
        let mut s3 = build_s3_tier("COLD-WASABI");
        let s3_config = s3.s3.as_mut().expect("S3 payload should exist");
        s3_config.endpoint = "https://s3.wasabisys.com".to_string();
        s3_config.bucket = "wasabi-bucket".to_string();
        s3_config.prefix = "archive".to_string();
        s3_config.region = "us-east-1".to_string();

        for (current_tier, replacement_tier) in [(s3.clone_with_credentials(), wasabi.clone_with_credentials()), (wasabi, s3)] {
            let mut current = empty_mgr();
            current.tiers.insert("COLD-WASABI".to_string(), current_tier);
            let mut replacement = empty_mgr();
            replacement.tiers.insert("COLD-WASABI".to_string(), replacement_tier);

            let err = replaced_tier_destinations(&current, &replacement)
                .expect_err("in-place type changes must not bypass mixed-version Wasabi fencing");
            assert!(err.message.contains("add a new tier instead"), "{}", err.message);
        }
    }

    #[test]
    fn backend_identity_v2_has_stable_fixtures() {
        assert_eq!(
            tier_backend_identity(&build_rustfs_tier("COLD-A")).expect("identity should encode"),
            [
                112, 73, 111, 29, 53, 43, 207, 7, 170, 12, 99, 116, 213, 135, 173, 227, 6, 220, 179, 135, 232, 83, 25, 13, 128,
                98, 254, 103, 132, 128, 229, 97,
            ]
        );
        assert_eq!(
            tier_backend_identity(&build_azure_tier("account-a")).expect("Azure identity should encode"),
            [
                57, 188, 231, 23, 184, 188, 233, 228, 118, 24, 158, 152, 178, 169, 2, 146, 149, 152, 104, 0, 146, 196, 61, 145,
                18, 1, 188, 20, 36, 215, 100, 125,
            ]
        );
    }

    #[test]
    fn destination_identity_prefix_normalization_matches_driver_matrix() {
        assert_eq!(normalized_tier_prefix(&TierType::S3, "/foo//"), "foo");
        assert_eq!(normalized_tier_prefix(&TierType::Wasabi, "/foo//"), "foo");
        for tier_type in [
            TierType::RustFS,
            TierType::MinIO,
            TierType::Aliyun,
            TierType::Tencent,
            TierType::Huaweicloud,
            TierType::Azure,
            TierType::GCS,
            TierType::R2,
        ] {
            assert_eq!(normalized_tier_prefix(&tier_type, "foo/"), "foo");
            assert_eq!(normalized_tier_prefix(&tier_type, "foo//"), "foo/");
        }
    }

    #[tokio::test]
    async fn direct_rollback_fails_closed_until_unpublished_generation_is_idle() {
        let manager = Arc::new(RwLock::new(empty_mgr()));
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("persisted"));
        }
        let persisted = HashMap::from([("COLD-A".to_string(), build_rustfs_tier("COLD-A"))]);

        let mut unpublished_config = build_rustfs_tier("COLD-A");
        unpublished_config
            .rustfs
            .as_mut()
            .expect("rustfs payload should exist")
            .endpoint = "https://unpublished.invalid".to_string();
        {
            let mut manager = manager.write().await;
            manager.tiers.insert("COLD-A".to_string(), unpublished_config);
            manager
                .replace_driver("COLD-A", Box::new(LeaseTestBackend::ready("unpublished")))
                .expect("unpublished driver should install before simulated save failure");
        }
        let unpublished = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("unpublished lease should be visible before rollback");

        let err = manager
            .write()
            .await
            .apply_reloaded_tiers(persisted.clone())
            .expect_err("direct rollback must not wait for an active generation");
        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(unpublished.is_current(&manager).await);
        drop(unpublished);
        manager
            .write()
            .await
            .apply_reloaded_tiers(persisted)
            .expect("rollback should apply once the generation is idle");

        let manager_guard = manager.read().await;
        assert_eq!(
            manager_guard
                .tiers
                .get("COLD-A")
                .and_then(|tier| tier.rustfs.as_ref())
                .expect("persisted tier should be restored")
                .endpoint,
            "https://example-compat.invalid"
        );
        let runtime = registered_tier_driver_runtime(&manager_guard).expect("runtime sidecar should remain registered");
        assert!(!lock_unpoisoned(&runtime).generations.contains_key("COLD-A"));
    }

    #[derive(Debug)]
    struct CasListBarrier {
        prefix: String,
        armed: AtomicBool,
        matches_before_pause: AtomicUsize,
        arrived: tokio::sync::Notify,
        release: tokio::sync::Semaphore,
    }

    #[derive(Debug)]
    struct CasPutFailure {
        prefix: String,
        successful_matches: usize,
        after_commit: bool,
    }

    #[derive(Debug, Default)]
    struct CasCoordinatorCommitBarrier {
        arrived: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    #[derive(Debug)]
    struct CasConfigStore {
        objects: tokio::sync::Mutex<HashMap<String, (Vec<u8>, String)>>,
        legacy_state: tokio::sync::Mutex<Option<Vec<u8>>>,
        if_match_race_rewrite: tokio::sync::Mutex<std::collections::VecDeque<(String, Vec<u8>)>>,
        next_etag: AtomicUsize,
        content_derived_etags: AtomicBool,
        fail_put: AtomicBool,
        fail_put_prefix: tokio::sync::Mutex<Option<CasPutFailure>>,
        fail_delete_prefix: tokio::sync::Mutex<Option<(String, usize)>>,
        delete_log: tokio::sync::Mutex<Vec<String>>,
        list_barrier: tokio::sync::Mutex<Option<Arc<CasListBarrier>>>,
        coordinator_commit_barrier: tokio::sync::Mutex<Option<Arc<CasCoordinatorCommitBarrier>>>,
        intent_list_calls: AtomicUsize,
        fail_reference_walk: AtomicBool,
        reference_walk_send_count: AtomicUsize,
        lock_manager: Arc<rustfs_lock::GlobalLockManager>,
        lock_requests: Mutex<Vec<(String, String)>>,
        listed_versions: Mutex<Vec<ObjectInfo>>,
        lifecycle_configs: Mutex<HashMap<String, BucketLifecycleConfiguration>>,
    }

    impl Default for CasConfigStore {
        fn default() -> Self {
            Self {
                objects: tokio::sync::Mutex::new(HashMap::new()),
                legacy_state: tokio::sync::Mutex::new(None),
                if_match_race_rewrite: tokio::sync::Mutex::new(std::collections::VecDeque::new()),
                next_etag: AtomicUsize::new(0),
                content_derived_etags: AtomicBool::new(false),
                fail_put: AtomicBool::new(false),
                fail_put_prefix: tokio::sync::Mutex::new(None),
                fail_delete_prefix: tokio::sync::Mutex::new(None),
                delete_log: tokio::sync::Mutex::new(Vec::new()),
                list_barrier: tokio::sync::Mutex::new(None),
                coordinator_commit_barrier: tokio::sync::Mutex::new(None),
                intent_list_calls: AtomicUsize::new(0),
                fail_reference_walk: AtomicBool::new(false),
                reference_walk_send_count: AtomicUsize::new(0),
                lock_manager: Arc::new(rustfs_lock::GlobalLockManager::new()),
                lock_requests: Mutex::new(Vec::new()),
                listed_versions: Mutex::new(Vec::new()),
                lifecycle_configs: Mutex::new(HashMap::new()),
            }
        }
    }

    impl CasConfigStore {
        fn with_content_derived_etags() -> Self {
            let store = Self::default();
            store.content_derived_etags.store(true, Ordering::SeqCst);
            store
        }

        fn next_object_etag(&self, payload: &[u8]) -> String {
            if self.content_derived_etags.load(Ordering::SeqCst) {
                let digest = md5::Md5::digest(payload);
                return hex_simd::encode_to_string(&digest[..], hex_simd::AsciiCase::Lower);
            }
            format!("etag-{}", self.next_etag.fetch_add(1, Ordering::SeqCst) + 1)
        }

        fn add_listed_version(&self, object: ObjectInfo) {
            self.listed_versions
                .lock()
                .expect("tier reference fixture should not poison")
                .push(object);
        }

        fn remove_listed_version(&self, bucket: &str, object: &str) {
            self.listed_versions
                .lock()
                .expect("tier reference fixture should not poison")
                .retain(|version| version.bucket != bucket || version.name != object);
        }

        fn add_lifecycle_config(&self, bucket: &str, config: BucketLifecycleConfiguration) {
            self.lifecycle_configs
                .lock()
                .expect("tier reference fixture should not poison")
                .insert(bucket.to_string(), config);
        }

        async fn insert_config_object(&self, object: String, data: Vec<u8>) {
            self.objects
                .lock()
                .await
                .insert(object, (data, "reference-proof-etag".to_string()));
        }

        async fn expire_coordinator_intent(&self, mut intent: TierMutationIntent) {
            use crate::services::tier::tier_mutation_intent::{
                TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX, TIER_MUTATION_INTENT_RECORD_PREFIX,
                tier_mutation_intent_record_object_name,
            };

            intent.expires_at_unix_nanos = 1;
            let peer_object = tier_mutation_intent_record_object_name(intent.mutation_id)
                .expect("coordinator intent fixture path should build");
            let coordinator_object =
                peer_object.replacen(TIER_MUTATION_INTENT_RECORD_PREFIX, TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX, 1);
            self.insert_config_object(
                coordinator_object,
                intent.encode().expect("expired coordinator intent fixture should encode"),
            )
            .await;
        }

        async fn rewrite_on_next_if_match(&self, object: String, data: Vec<u8>) {
            self.if_match_race_rewrite.lock().await.push_back((object, data));
        }

        fn fail_reference_walk(&self) {
            self.fail_reference_walk.store(true, Ordering::SeqCst);
        }

        fn reference_walk_send_count(&self) -> usize {
            self.reference_walk_send_count.load(Ordering::SeqCst)
        }

        async fn fail_next_delete_with_prefix(&self, prefix: &str) {
            self.fail_delete_with_prefix_after(prefix, 0).await;
        }

        async fn fail_delete_with_prefix_after(&self, prefix: &str, successful_matches: usize) {
            *self.fail_delete_prefix.lock().await = Some((prefix.to_string(), successful_matches));
        }

        async fn fail_put_with_prefix_after(&self, prefix: &str, successful_matches: usize, after_commit: bool) {
            *self.fail_put_prefix.lock().await = Some(CasPutFailure {
                prefix: prefix.to_string(),
                successful_matches,
                after_commit,
            });
        }

        async fn delete_log(&self) -> Vec<String> {
            self.delete_log.lock().await.clone()
        }

        fn intent_list_calls(&self) -> usize {
            self.intent_list_calls.load(Ordering::SeqCst)
        }

        async fn pause_next_list_with_prefix(&self, prefix: &str) -> Arc<CasListBarrier> {
            self.pause_list_with_prefix_after(prefix, 0).await
        }

        async fn pause_list_with_prefix_after(&self, prefix: &str, successful_matches: usize) -> Arc<CasListBarrier> {
            let barrier = Arc::new(CasListBarrier {
                prefix: prefix.to_string(),
                armed: AtomicBool::new(true),
                matches_before_pause: AtomicUsize::new(successful_matches),
                arrived: tokio::sync::Notify::new(),
                release: tokio::sync::Semaphore::new(0),
            });
            *self.list_barrier.lock().await = Some(barrier.clone());
            barrier
        }
    }

    #[async_trait::async_trait]
    impl ObjectIO for CasConfigStore {
        type Error = Error;
        type RangeSpec = HTTPRangeSpec;
        type HeaderMap = HeaderMap;
        type ObjectOptions = ObjectOptions;
        type ObjectInfo = ObjectInfo;
        type GetObjectReader = GetObjectReader;
        type PutObjectReader = PutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<Self::RangeSpec>,
            _headers: Self::HeaderMap,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::GetObjectReader> {
            let legacy_config_path = tier_config_path(TIER_CONFIG_LEGACY_FILE);
            let (data, etag) = if object == legacy_config_path {
                let state = self.legacy_state.lock().await;
                let data = state.as_ref().ok_or(Error::ConfigNotFound)?;
                (data.clone(), "legacy-config-etag".to_string())
            } else {
                let objects = self.objects.lock().await;
                let (data, etag) = objects.get(object).ok_or(Error::ConfigNotFound)?;
                (data.clone(), etag.clone())
            };
            Ok(GetObjectReader {
                stream: Box::new(Cursor::new(data.clone())),
                object_info: ObjectInfo {
                    bucket: bucket.to_string(),
                    name: object.to_string(),
                    size: data.len() as i64,
                    actual_size: data.len() as i64,
                    etag: Some(etag),
                    ..Default::default()
                },
                buffered_body: None,
                body_source: Default::default(),
            })
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            data: &mut Self::PutObjectReader,
            opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo> {
            if self.fail_put.load(Ordering::SeqCst) {
                return Err(Error::other("injected tier config save failure"));
            }
            let injected_failure = {
                let mut failure = self.fail_put_prefix.lock().await;
                let matching = failure.as_ref().is_some_and(|failure| object.starts_with(&failure.prefix));
                if matching && failure.as_ref().is_some_and(|failure| failure.successful_matches == 0) {
                    failure.take().map(|failure| failure.after_commit)
                } else {
                    if matching {
                        failure
                            .as_mut()
                            .expect("matching put failure should exist")
                            .successful_matches -= 1;
                    }
                    None
                }
            };
            if injected_failure == Some(false) {
                return Err(Error::other("injected pre-commit tier config save failure"));
            }
            let mut payload = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut data.stream, &mut payload).await?;
            if object.starts_with(crate::services::tier::tier_mutation_intent::TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX)
                && opts
                    .http_preconditions
                    .as_ref()
                    .and_then(HTTPPreconditions::if_match_value)
                    .is_some()
            {
                let barrier = self.coordinator_commit_barrier.lock().await.take();
                if let Some(barrier) = barrier {
                    barrier.arrived.notify_one();
                    barrier.release.notified().await;
                }
            }
            let race_rewrite = if opts
                .http_preconditions
                .as_ref()
                .and_then(HTTPPreconditions::if_match_value)
                .is_some()
            {
                self.if_match_race_rewrite.lock().await.pop_front()
            } else {
                None
            };
            let mut objects = self.objects.lock().await;
            if let Some((target, data)) = race_rewrite
                && target == object
            {
                let etag = self.next_object_etag(&data);
                objects.insert(object.to_string(), (data, etag));
            }
            match objects.get(object) {
                Some((_, etag)) => opts.precondition_check(&ObjectInfo {
                    etag: Some(etag.clone()),
                    ..Default::default()
                })?,
                None => {
                    if opts
                        .http_preconditions
                        .as_ref()
                        .and_then(HTTPPreconditions::if_match_value)
                        .is_some()
                    {
                        return Err(Error::ObjectNotFound(bucket.to_string(), object.to_string()));
                    }
                }
            }
            let etag = self.next_object_etag(&payload);
            objects.insert(object.to_string(), (payload.clone(), etag.clone()));
            let info = ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                size: payload.len() as i64,
                actual_size: payload.len() as i64,
                etag: Some(etag),
                ..Default::default()
            };
            if injected_failure == Some(true) {
                return Err(Error::other("injected post-commit tier config response failure"));
            }
            Ok(info)
        }
    }

    #[async_trait::async_trait]
    impl ObjectOperations for CasConfigStore {
        type Error = Error;
        type ObjectInfo = ObjectInfo;
        type ObjectOptions = ObjectOptions;
        type FileInfo = FileInfo;
        type ObjectToDelete = ObjectToDelete;
        type DeletedObject = DeletedObject;

        async fn get_object_info(&self, bucket: &str, object: &str, _opts: &Self::ObjectOptions) -> Result<Self::ObjectInfo> {
            let objects = self.objects.lock().await;
            let (data, etag) = objects
                .get(object)
                .ok_or(Error::ObjectNotFound(bucket.to_string(), object.to_string()))?;
            Ok(ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                size: data.len() as i64,
                actual_size: data.len() as i64,
                etag: Some(etag.clone()),
                ..Default::default()
            })
        }

        async fn verify_object_integrity(&self, _bucket: &str, _object: &str, _opts: &Self::ObjectOptions) -> Result<()> {
            Err(Error::NotImplemented)
        }

        async fn copy_object(
            &self,
            _src_bucket: &str,
            _src_object: &str,
            _dst_bucket: &str,
            _dst_object: &str,
            _src_info: &mut Self::ObjectInfo,
            _src_opts: &Self::ObjectOptions,
            _dst_opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo> {
            Err(Error::NotImplemented)
        }

        async fn delete_object_version(
            &self,
            _bucket: &str,
            _object: &str,
            _fi: &Self::FileInfo,
            _force_del_marker: bool,
        ) -> Result<()> {
            Err(Error::NotImplemented)
        }

        async fn delete_object(&self, bucket: &str, object: &str, opts: Self::ObjectOptions) -> Result<Self::ObjectInfo> {
            self.delete_log.lock().await.push(object.to_string());
            let mut fail_delete_prefix = self.fail_delete_prefix.lock().await;
            let fail_now = match fail_delete_prefix.as_mut() {
                Some((prefix, successful_matches)) if object.starts_with(prefix.as_str()) => {
                    if *successful_matches == 0 {
                        true
                    } else {
                        *successful_matches -= 1;
                        false
                    }
                }
                _ => false,
            };
            if fail_now {
                fail_delete_prefix.take();
                return Err(Error::other("injected tier mutation intent delete failure"));
            }
            drop(fail_delete_prefix);
            let race_rewrite = if opts
                .http_preconditions
                .as_ref()
                .and_then(HTTPPreconditions::if_match_value)
                .is_some()
            {
                self.if_match_race_rewrite.lock().await.pop_front()
            } else {
                None
            };
            let mut objects = self.objects.lock().await;
            if let Some((target, data)) = race_rewrite
                && target == object
            {
                let etag = self.next_object_etag(&data);
                objects.insert(object.to_string(), (data, etag));
            }
            let (data, etag) = objects
                .get(object)
                .cloned()
                .ok_or_else(|| Error::ObjectNotFound(bucket.to_string(), object.to_string()))?;
            opts.precondition_check(&ObjectInfo {
                etag: Some(etag.clone()),
                ..Default::default()
            })?;
            objects.remove(object);
            Ok(ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                size: data.len() as i64,
                actual_size: data.len() as i64,
                etag: Some(etag),
                ..Default::default()
            })
        }

        async fn delete_objects(
            &self,
            _bucket: &str,
            _objects: Vec<Self::ObjectToDelete>,
            _opts: Self::ObjectOptions,
        ) -> (Vec<Self::DeletedObject>, Vec<Option<Self::Error>>) {
            (Vec::new(), vec![Some(Error::NotImplemented)])
        }

        async fn put_object_metadata(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo> {
            Err(Error::NotImplemented)
        }

        async fn get_object_tags(&self, _bucket: &str, _object: &str, _opts: &Self::ObjectOptions) -> Result<String> {
            Err(Error::NotImplemented)
        }

        async fn put_object_tags(
            &self,
            _bucket: &str,
            _object: &str,
            _tags: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo> {
            Err(Error::NotImplemented)
        }

        async fn delete_object_tags(
            &self,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<Self::ObjectInfo> {
            Err(Error::NotImplemented)
        }

        async fn add_partial(&self, _bucket: &str, _object: &str, _version_id: &str) -> Result<()> {
            Err(Error::NotImplemented)
        }

        async fn transition_object(&self, _bucket: &str, _object: &str, _opts: &Self::ObjectOptions) -> Result<()> {
            Err(Error::NotImplemented)
        }

        async fn restore_transitioned_object(
            self: Arc<Self>,
            _bucket: &str,
            _object: &str,
            _opts: &Self::ObjectOptions,
        ) -> Result<()> {
            Err(Error::NotImplemented)
        }
    }

    #[async_trait::async_trait]
    impl BucketOperations for CasConfigStore {
        type Error = Error;

        async fn make_bucket(
            &self,
            _bucket: &str,
            _opts: &crate::storage_api_contracts::bucket::MakeBucketOptions,
        ) -> Result<()> {
            Err(Error::NotImplemented)
        }

        async fn get_bucket_info(
            &self,
            _bucket: &str,
            _opts: &crate::storage_api_contracts::bucket::BucketOptions,
        ) -> Result<crate::storage_api_contracts::bucket::BucketInfo> {
            Err(Error::NotImplemented)
        }

        async fn list_bucket(
            &self,
            _opts: &crate::storage_api_contracts::bucket::BucketOptions,
        ) -> Result<Vec<crate::storage_api_contracts::bucket::BucketInfo>> {
            let mut seen = HashSet::new();
            let mut buckets = Vec::new();
            for object in self
                .listed_versions
                .lock()
                .expect("tier reference fixture should not poison")
                .iter()
            {
                if seen.insert(object.bucket.clone()) {
                    buckets.push(crate::storage_api_contracts::bucket::BucketInfo {
                        name: object.bucket.clone(),
                        ..Default::default()
                    });
                }
            }
            Ok(buckets)
        }

        async fn delete_bucket(
            &self,
            _bucket: &str,
            _opts: &crate::storage_api_contracts::bucket::DeleteBucketOptions,
        ) -> Result<()> {
            Err(Error::NotImplemented)
        }
    }

    #[async_trait::async_trait]
    impl ListOperations for CasConfigStore {
        type Error = Error;
        type ListObjectsV2Info = StorageListObjectsV2Info<ObjectInfo>;
        type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;
        type ObjectInfoOrErr = StorageObjectInfoOrErr<ObjectInfo, Error>;
        type WalkOptions = TierReferenceProofWalkOptions;
        type WalkCancellation = tokio_util::sync::CancellationToken;
        type WalkResultSender = tokio::sync::mpsc::Sender<StorageObjectInfoOrErr<ObjectInfo, Error>>;

        async fn list_objects_v2(
            self: Arc<Self>,
            bucket: &str,
            prefix: &str,
            continuation_token: Option<String>,
            _delimiter: Option<String>,
            max_keys: i32,
            _fetch_owner: bool,
            _start_after: Option<String>,
            _incl_deleted: bool,
        ) -> Result<Self::ListObjectsV2Info> {
            if matches!(
                prefix,
                crate::services::tier::tier_mutation_intent::TIER_MUTATION_INTENT_RECORD_PREFIX
                    | crate::services::tier::tier_mutation_intent::TIER_COORDINATOR_MUTATION_INTENT_RECORD_PREFIX
            ) {
                self.intent_list_calls.fetch_add(1, Ordering::SeqCst);
            }
            let list_barrier = self.list_barrier.lock().await.clone();
            if let Some(barrier) = list_barrier
                && prefix.starts_with(&barrier.prefix)
            {
                let should_pause =
                    match barrier
                        .matches_before_pause
                        .try_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| remaining.checked_sub(1))
                    {
                        Ok(_) => false,
                        Err(_) => barrier.armed.swap(false, Ordering::SeqCst),
                    };
                if should_pause {
                    barrier.arrived.notify_one();
                    barrier
                        .release
                        .acquire()
                        .await
                        .expect("list test barrier should stay open")
                        .forget();
                }
            }
            let mut objects = if bucket == RUSTFS_META_BUCKET {
                self.objects
                    .lock()
                    .await
                    .keys()
                    .filter(|object| object.starts_with(prefix))
                    .map(|object| ObjectInfo {
                        bucket: bucket.to_string(),
                        name: object.clone(),
                        ..Default::default()
                    })
                    .collect::<Vec<_>>()
            } else {
                Vec::new()
            };
            objects.sort_by(|left, right| left.name.cmp(&right.name));
            if let Some(marker) = continuation_token {
                objects.retain(|object| object.name > marker);
            }
            let limit = usize::try_from(max_keys).unwrap_or(0);
            let is_truncated = objects.len() > limit;
            if is_truncated {
                objects.truncate(limit);
            }
            let next_continuation_token = is_truncated
                .then(|| objects.last().map(|object| object.name.clone()))
                .flatten();
            Ok(StorageListObjectsV2Info {
                objects,
                is_truncated,
                next_continuation_token,
                ..Default::default()
            })
        }

        async fn list_object_versions(
            self: Arc<Self>,
            bucket: &str,
            prefix: &str,
            marker: Option<String>,
            version_marker: Option<String>,
            _delimiter: Option<String>,
            max_keys: i32,
        ) -> Result<Self::ListObjectVersionsInfo> {
            let mut objects: Vec<_> = self
                .listed_versions
                .lock()
                .expect("tier reference fixture should not poison")
                .iter()
                .filter(|object| object.bucket == bucket && object.name.starts_with(prefix))
                .cloned()
                .collect();
            objects.sort_by_key(tier_test_object_marker);
            if marker.is_some() || version_marker.is_some() {
                let marker = (marker.unwrap_or_default(), version_marker.unwrap_or_default());
                objects.retain(|object| tier_test_object_marker(object) > marker);
            }
            let limit: usize = usize::try_from(max_keys).unwrap_or_default();
            let is_truncated = objects.len() > limit;
            if is_truncated {
                objects.truncate(limit);
            }
            let (next_marker, next_version_idmarker) = if is_truncated {
                objects
                    .last()
                    .map(|object| (Some(object.name.clone()), object.version_id.map(|version| version.to_string())))
                    .unwrap_or((None, None))
            } else {
                (None, None)
            };
            Ok(StorageListObjectVersionsInfo {
                is_truncated,
                next_marker,
                next_version_idmarker,
                objects,
                prefixes: Vec::new(),
            })
        }

        async fn walk(
            self: Arc<Self>,
            rx: Self::WalkCancellation,
            bucket: &str,
            prefix: &str,
            result: Self::WalkResultSender,
            opts: Self::WalkOptions,
        ) -> Result<()> {
            if self.fail_reference_walk.load(Ordering::SeqCst)
                && result
                    .send(StorageObjectInfoOrErr {
                        item: None,
                        err: Some(Error::other("injected tier reference walk failure")),
                    })
                    .await
                    .is_err()
            {
                return Ok(());
            }
            let mut objects = self
                .listed_versions
                .lock()
                .expect("tier reference fixture should not poison")
                .iter()
                .filter(|object| object.bucket == bucket && object.name.starts_with(prefix))
                .filter(|object| opts.include_free_versions || !object.transitioned_object.free_version)
                .cloned()
                .collect::<Vec<_>>();
            objects.sort_by_key(tier_test_object_marker);
            if let Some(marker) = opts.marker.as_deref() {
                objects.retain(|object| object.name.as_str() > marker);
            }
            if opts.limit > 0 {
                objects.truncate(opts.limit);
            }
            for object in objects {
                if rx.is_cancelled() {
                    return Err(Error::other("tier reference fixture walk cancelled"));
                }
                self.reference_walk_send_count.fetch_add(1, Ordering::SeqCst);
                if result
                    .send(StorageObjectInfoOrErr {
                        item: Some(object),
                        err: None,
                    })
                    .await
                    .is_err()
                {
                    return Ok(());
                }
            }
            Ok(())
        }
    }

    fn tier_test_object_marker(object: &ObjectInfo) -> (String, String) {
        (
            object.name.clone(),
            object.version_id.map(|version| version.to_string()).unwrap_or_default(),
        )
    }

    fn transitioned_tier_object(
        bucket: &str,
        object: &str,
        tier_name: &str,
        backend_identity: Option<TierDestinationId>,
    ) -> ObjectInfo {
        let mut user_defined = HashMap::new();
        if let Some(identity) = backend_identity {
            rustfs_utils::http::metadata_compat::insert_str(
                &mut user_defined,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
                rustfs_utils::crypto::hex(identity),
            );
        }
        ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            user_defined: Arc::new(user_defined),
            transitioned_object: crate::storage_api_contracts::lifecycle::TransitionedObject {
                name: object.to_string(),
                tier: tier_name.to_string(),
                status: rustfs_filemeta::TRANSITION_COMPLETE.to_string(),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[async_trait::async_trait]
    impl NamespaceLocking for CasConfigStore {
        type Error = Error;
        type NamespaceLock = rustfs_lock::NamespaceLockWrapper;

        async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<Self::NamespaceLock> {
            self.lock_requests
                .lock()
                .expect("tier config lock request log should not poison")
                .push((bucket.to_string(), object.to_string()));
            let lock =
                rustfs_lock::NamespaceLock::with_local_manager("tier-config-update-test".to_string(), self.lock_manager.clone());
            Ok(rustfs_lock::NamespaceLockWrapper::new(
                lock,
                rustfs_lock::ObjectKey::new(bucket.to_string(), object.to_string()),
                "tier-config-update-test-owner".to_string(),
            ))
        }
    }

    #[async_trait::async_trait]
    impl TierReferenceProofStore for CasConfigStore {
        async fn lifecycle_config_for_reference_proof(&self, bucket: &str) -> Result<Option<BucketLifecycleConfiguration>> {
            Ok(self
                .lifecycle_configs
                .lock()
                .expect("tier reference fixture should not poison")
                .get(bucket)
                .cloned())
        }
    }

    #[tokio::test]
    async fn edit_and_save_with_omitted_credentials_preserves_persisted_and_runtime_secrets() {
        const ACCESS_KEY: &str = "persisted-access-value";
        const SECRET_KEY: &str = "persisted-secret-value";

        let store = Arc::new(CasConfigStore::default());
        let mut tier = build_rustfs_tier("COLD-R");
        let rustfs = tier.rustfs.as_mut().expect("RustFS payload should exist");
        rustfs.access_key = ACCESS_KEY.to_string();
        rustfs.secret_key = SECRET_KEY.to_string();
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-R".to_string(), tier);
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("credential-preservation fixture should persist");

        let manager = TierConfigMgr::new();
        let backend = RecordingWarmBackend::new();
        let observed = Arc::new(Mutex::new(Vec::new()));
        TIER_DRIVER_TEST_FACTORY
            .scope(
                recording_driver_factory(backend.clone(), observed.clone()),
                TIER_MUTATION_TEST_PEERS.scope(
                    Vec::new(),
                    TierConfigMgr::edit_and_save_with(&manager, store.clone(), "COLD-R", TierCreds::default()),
                ),
            )
            .await
            .expect("omitted credentials should preserve and publish the current credentials");

        let reloaded = load_tier_config_for_update(store.clone())
            .await
            .expect("edited tier config should reload")
            .0;
        let persisted_rustfs = reloaded.tiers["COLD-R"]
            .rustfs
            .as_ref()
            .expect("persisted RustFS payload should exist");
        assert_eq!(persisted_rustfs.access_key, ACCESS_KEY);
        assert_eq!(persisted_rustfs.secret_key, SECRET_KEY);

        let config_object = store
            .objects
            .lock()
            .await
            .get(&tier_config_path(TIER_CONFIG_FILE))
            .expect("binary tier config should remain persisted")
            .0
            .clone();
        assert!(
            !config_object
                .windows(TIER_CREDENTIAL_REDACTED.len())
                .any(|window| window == TIER_CREDENTIAL_REDACTED.as_bytes()),
            "the API placeholder must never be persisted as a credential"
        );

        let (runtime_access_key, runtime_secret_key) = {
            let manager = manager.read().await;
            let runtime = registered_tier_driver_runtime(&manager).expect("driver runtime should be registered");
            let runtime = lock_unpoisoned(&runtime);
            let generation = runtime
                .generations
                .get("COLD-R")
                .expect("edited generation should be installed");
            let rustfs = generation
                .tier_config
                .rustfs
                .as_ref()
                .expect("runtime RustFS payload should exist");
            (rustfs.access_key.clone(), rustfs.secret_key.clone())
        };
        assert_eq!(runtime_access_key, ACCESS_KEY);
        assert_eq!(runtime_secret_key, SECRET_KEY);

        let api_view = manager
            .read()
            .await
            .get("COLD-R")
            .expect("edited tier should remain visible through the admin view");
        assert_eq!(
            api_view.rustfs.expect("admin RustFS payload should exist").secret_key,
            TIER_CREDENTIAL_REDACTED
        );
        {
            let observed = lock_unpoisoned(&observed);
            assert_eq!(observed.len(), 1);
            assert_eq!(
                observed[0]
                    .rustfs
                    .as_ref()
                    .expect("backend factory should observe the RustFS payload")
                    .secret_key,
                SECRET_KEY
            );
        }

        let operations = backend.op_log().await;
        assert_eq!(operations.len(), 5);
        assert!(matches!(&operations[0], MockWarmOp::Put { .. }));
        assert!(matches!(&operations[1], MockWarmOp::Probe { .. }));
        assert!(matches!(&operations[2], MockWarmOp::Get { .. }));
        assert!(matches!(&operations[3], MockWarmOp::Remove { .. }));
        assert!(matches!(&operations[4], MockWarmOp::Probe { .. }));
    }

    #[tokio::test]
    async fn edit_and_save_rotates_credentials_while_an_active_lifecycle_rule_references_the_tier() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("credential rotation fixture should persist");
        store.add_listed_version(ObjectInfo {
            bucket: "photos".to_string(),
            name: "safe.txt".to_string(),
            ..Default::default()
        });
        store.add_lifecycle_config(
            "photos",
            BucketLifecycleConfiguration {
                expiry_updated_at: None,
                rules: vec![LifecycleRule {
                    status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                    expiration: None,
                    abort_incomplete_multipart_upload: None,
                    del_marker_expiration: None,
                    filter: None,
                    id: Some("move-current".to_string()),
                    noncurrent_version_expiration: None,
                    noncurrent_version_transitions: None,
                    prefix: None,
                    transitions: Some(vec![Transition {
                        days: Some(1),
                        date: None,
                        storage_class: Some(TransitionStorageClass::from_static("COLD-A")),
                    }]),
                }],
            },
        );

        let manager = TierConfigMgr::new();
        let backend = RecordingWarmBackend::new();
        TIER_DRIVER_TEST_FACTORY
            .scope(
                recording_driver_factory(backend, Arc::new(Mutex::new(Vec::new()))),
                TIER_MUTATION_TEST_PEERS.scope(
                    Vec::new(),
                    TierConfigMgr::edit_and_save_with(
                        &manager,
                        store.clone(),
                        "COLD-A",
                        TierCreds {
                            access_key: "rotated-access".to_string(),
                            secret_key: "rotated-secret".to_string(),
                            ..Default::default()
                        },
                    ),
                ),
            )
            .await
            .expect("same-destination credential rotation must not be treated as a lifecycle rebind");

        let reloaded = load_tier_config_for_update(store)
            .await
            .expect("rotated tier config should reload")
            .0;
        let rustfs = reloaded.tiers["COLD-A"]
            .rustfs
            .as_ref()
            .expect("RustFS payload should remain");
        assert_eq!(rustfs.access_key, "rotated-access");
        assert_eq!(rustfs.secret_key, "rotated-secret");
    }

    async fn assert_edit_rejected_before_distributed_commit(
        backend: RecordingWarmBackend,
        credentials: TierCreds,
        expected_code: &str,
        expected_backend_builds: usize,
    ) {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("rejected-edit fixture should persist");
        let before = store
            .objects
            .lock()
            .await
            .get(&tier_config_path(TIER_CONFIG_FILE))
            .expect("base config should be present")
            .clone();

        let manager = TierConfigMgr::new();
        {
            let mut manager = manager.write().await;
            install_lease_backend(&mut manager, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old_generation = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old generation should be available")
            .generation();
        let peer_calls = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::new(Mutex::new(Vec::new()));
        let err = TIER_DRIVER_TEST_FACTORY
            .scope(
                recording_driver_factory(backend, observed.clone()),
                TIER_MUTATION_TEST_PEERS.scope(
                    vec![FakeTierMutationPeer::boxed(
                        "peer-a",
                        peer_calls.clone(),
                        Ok(PeerTierMutationState::Committed),
                    )],
                    TierConfigMgr::edit_and_save_with(&manager, store.clone(), "COLD-A", credentials),
                ),
            )
            .await
            .expect_err("credential validation must fail before distributed prepare");

        let TierConfigUpdateError::Mutation(err) = err else {
            panic!("credential validation should be reported as a mutation error: {err:?}");
        };
        assert_eq!(err.code, expected_code);
        assert!(lock_unpoisoned(&peer_calls).is_empty());
        assert_eq!(lock_unpoisoned(&observed).len(), expected_backend_builds);
        assert_eq!(
            store
                .objects
                .lock()
                .await
                .get(&tier_config_path(TIER_CONFIG_FILE))
                .expect("base config should remain present"),
            &before
        );
        let current = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("the old generation should be restored after validation failure");
        assert_eq!(current.generation(), old_generation);
        assert_eq!(
            current
                .inner
                .tier_config
                .rustfs
                .as_ref()
                .expect("runtime RustFS payload should remain")
                .secret_key,
            "sk"
        );
    }

    #[tokio::test]
    async fn edit_and_save_rejects_half_credentials_before_peer_prepare_or_config_cas() {
        assert_edit_rejected_before_distributed_commit(
            RecordingWarmBackend::new(),
            TierCreds {
                access_key: "rotated-access".to_string(),
                ..Default::default()
            },
            &ERR_TIER_MISSING_CREDENTIALS.code,
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn edit_and_save_rejects_canonical_azure_service_principal_before_peer_prepare() {
        let credentials: TierCreds = serde_json::from_value(serde_json::json!({
            "azSP": {
                "TenantID": "tenant",
                "ClientID": "client",
                "ClientSecret": "service-principal-secret"
            }
        }))
        .expect("canonical madmin credentials should decode");
        assert_edit_rejected_before_distributed_commit(
            RecordingWarmBackend::new(),
            credentials,
            &ERR_TIER_INVALID_CONFIG.code,
            0,
        )
        .await;
    }

    #[tokio::test]
    async fn edit_and_save_rejects_legacy_azure_options_before_probe_or_peer_prepare() {
        let store = Arc::new(CasConfigStore::default());
        let mut legacy = build_azure_tier("account-a");
        legacy.azure.as_mut().expect("Azure payload should exist").storage_class = "HOT".to_string();
        let mut persisted = empty_mgr();
        persisted
            .tiers
            .insert("COLD-AZURE".to_string(), legacy.clone_with_credentials());
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("legacy Azure fixture should persist");
        let before = store
            .objects
            .lock()
            .await
            .get(&tier_config_path(TIER_CONFIG_FILE))
            .expect("base config should be present")
            .clone();

        let manager = TierConfigMgr::new();
        {
            let mut manager = manager.write().await;
            manager.tiers.insert("COLD-AZURE".to_string(), legacy);
            manager
                .replace_driver("COLD-AZURE", Box::new(LeaseTestBackend::ready("old")))
                .expect("legacy generation should install");
        }
        let old_generation = TierConfigMgr::acquire_operation_lease(&manager, "COLD-AZURE")
            .await
            .expect("legacy generation should be available")
            .generation();
        let peer_calls = Arc::new(Mutex::new(Vec::new()));
        let observed = Arc::new(Mutex::new(Vec::new()));

        let err = TIER_DRIVER_TEST_FACTORY
            .scope(
                recording_driver_factory(RecordingWarmBackend::new(), observed.clone()),
                TIER_MUTATION_TEST_PEERS.scope(
                    vec![FakeTierMutationPeer::boxed(
                        "peer-a",
                        peer_calls.clone(),
                        Ok(PeerTierMutationState::Committed),
                    )],
                    TierConfigMgr::edit_and_save_with(&manager, store.clone(), "COLD-AZURE", TierCreds::default()),
                ),
            )
            .await
            .expect_err("legacy unsupported Azure options must fail closed on edit");

        let TierConfigUpdateError::Mutation(err) = err else {
            panic!("legacy Azure validation should be a mutation error: {err:?}");
        };
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert!(err.message.contains("storageClass"), "{}", err.message);
        assert!(lock_unpoisoned(&observed).is_empty(), "validation must precede backend preparation");
        assert!(lock_unpoisoned(&peer_calls).is_empty(), "validation must precede peer prepare");
        assert_eq!(
            store
                .objects
                .lock()
                .await
                .get(&tier_config_path(TIER_CONFIG_FILE))
                .expect("base config should remain present"),
            &before
        );
        let current = TierConfigMgr::acquire_operation_lease(&manager, "COLD-AZURE")
            .await
            .expect("legacy generation should remain available");
        assert_eq!(current.generation(), old_generation);
        assert_eq!(
            current
                .inner
                .tier_config
                .azure
                .as_ref()
                .expect("runtime Azure payload should remain")
                .storage_class,
            "HOT"
        );
    }

    #[tokio::test]
    async fn edit_and_save_probe_failure_keeps_old_generation_and_skips_peer_prepare() {
        let backend = RecordingWarmBackend::new();
        backend.set_reject_credentials(true).await;
        assert_edit_rejected_before_distributed_commit(
            backend,
            TierCreds {
                access_key: "rotated-access".to_string(),
                secret_key: "rotated-secret".to_string(),
                ..Default::default()
            },
            &ERR_TIER_PERM_ERR.code,
            1,
        )
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn edit_and_save_get_timeout_cleans_probe_and_keeps_config_and_generation() {
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("timeout fixture should persist");
        let before = store
            .objects
            .lock()
            .await
            .get(&tier_config_path(TIER_CONFIG_FILE))
            .expect("base config should be present")
            .clone();

        let manager = TierConfigMgr::new();
        {
            let mut manager = manager.write().await;
            install_lease_backend(&mut manager, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old_generation = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old generation should be available")
            .generation();
        let backend = RecordingWarmBackend::new();
        let get_barrier = backend.arm_get_barrier().await;
        let peer_calls = Arc::new(Mutex::new(Vec::new()));
        let edit = TIER_DRIVER_TEST_FACTORY.scope(
            recording_driver_factory(backend.clone(), Arc::new(Mutex::new(Vec::new()))),
            TIER_MUTATION_TEST_PEERS.scope(
                vec![FakeTierMutationPeer::boxed(
                    "peer-a",
                    peer_calls.clone(),
                    Ok(PeerTierMutationState::Committed),
                )],
                TierConfigMgr::edit_and_save_with(
                    &manager,
                    store.clone(),
                    "COLD-A",
                    TierCreds {
                        access_key: "rotated-access".to_string(),
                        secret_key: "rotated-secret".to_string(),
                        ..Default::default()
                    },
                ),
            ),
        );
        tokio::pin!(edit);
        tokio::select! {
            _ = get_barrier.wait_until_paused() => {}
            result = &mut edit => panic!("edit completed before the probe GET timeout: {result:?}"),
        }
        tokio::time::advance(WARM_BACKEND_PROBE_TIMEOUT + Duration::from_millis(1)).await;
        let err = edit.await.expect_err("a hanging probe GET must time out");
        let TierConfigUpdateError::Mutation(err) = err else {
            panic!("probe timeout should be reported as a mutation error: {err:?}");
        };
        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(lock_unpoisoned(&peer_calls).is_empty());
        assert_eq!(
            store
                .objects
                .lock()
                .await
                .get(&tier_config_path(TIER_CONFIG_FILE))
                .expect("base config should remain present"),
            &before
        );
        assert_eq!(
            TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
                .await
                .expect("old generation should remain available")
                .generation(),
            old_generation
        );
        let operations = backend.op_log().await;
        assert!(matches!(operations.first(), Some(MockWarmOp::Put { .. })));
        assert!(
            operations
                .iter()
                .any(|operation| matches!(operation, MockWarmOp::Remove { .. }))
        );
        assert!(matches!(operations.last(), Some(MockWarmOp::Probe { .. })));
    }

    #[tokio::test]
    async fn remove_and_clear_full_update_paths_preserve_force() {
        let remove_store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(remove_store.clone(), None)
            .await
            .expect("remove fixture should persist");
        let remove_manager = TierConfigMgr::new();
        {
            let mut guard = remove_manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::in_use("remove"));
        }
        TierConfigMgr::remove_and_save_with(&remove_manager, remove_store.clone(), "COLD-A", true)
            .await
            .expect("forced remove must complete through load, mutate, save, and publish");
        assert!(!remove_manager.read().await.tiers.contains_key("COLD-A"));
        assert!(
            load_tier_config_for_update(remove_store)
                .await
                .expect("removed config should reload")
                .0
                .tiers
                .is_empty(),
            "forced remove must persist the empty candidate"
        );

        let clear_store = Arc::new(CasConfigStore::default());
        persisted
            .save_tiering_config_if_current(clear_store.clone(), None)
            .await
            .expect("clear fixture should persist");
        let clear_manager = TierConfigMgr::new();
        {
            let mut guard = clear_manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::in_use("clear"));
        }
        TierConfigMgr::clear_and_save_with(&clear_manager, clear_store.clone(), true)
            .await
            .expect("forced clear must complete through load, mutate, save, and publish");
        assert!(clear_manager.read().await.tiers.is_empty());
        assert!(
            load_tier_config_for_update(clear_store)
                .await
                .expect("cleared config should reload")
                .0
                .tiers
                .is_empty(),
            "forced clear must persist the empty candidate"
        );
    }

    #[tokio::test]
    async fn zero_reference_proof_blocks_remove_before_config_save() {
        let store = Arc::new(CasConfigStore::default());
        let tier = build_rustfs_tier("COLD-A");
        let identity = tier_backend_identity(&tier).expect("test tier identity should encode");
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), tier.clone_with_credentials());
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("reference proof fixture should persist");
        store.add_listed_version(transitioned_tier_object("photos", "2026/a.jpg", "COLD-A", Some(identity)));
        for index in 0..500 {
            store.add_listed_version(ObjectInfo {
                bucket: "photos".to_string(),
                name: format!("zz-safe/{index:04}.jpg"),
                ..Default::default()
            });
        }

        let manager = TierConfigMgr::new();
        manager.write().await.tiers.insert("COLD-A".to_string(), tier);
        let err = TierConfigMgr::remove_and_save_with(&manager, store.clone(), "COLD-A", true)
            .await
            .expect_err("authoritative object reference must block tier removal");

        match err {
            TierConfigUpdateError::Publish(err) => {
                assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
                assert!(err.message.contains("photos/2026/a.jpg"), "{}", err.message);
            }
            other => panic!("reference proof failures must be surfaced as publish errors: {other:?}"),
        }
        assert!(manager.read().await.tiers.contains_key("COLD-A"));
        assert!(
            store.reference_walk_send_count() < 501,
            "the proof should cancel its internal walk after the first authoritative blocker"
        );
        assert!(
            load_tier_config_for_update(store)
                .await
                .expect("blocked config should still reload")
                .0
                .tiers
                .contains_key("COLD-A"),
            "blocked removal must not persist the empty candidate"
        );
    }

    #[tokio::test]
    async fn tier_remove_prepared_fence_allows_inflight_free_version_cleanup_to_finish() {
        let store = Arc::new(CasConfigStore::default());
        let tier = build_rustfs_tier("COLD-A");
        let identity = tier_backend_identity(&tier).expect("test tier identity should encode");
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), tier.clone_with_credentials());
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("free-version drain fixture should persist");

        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            guard.tiers.insert("COLD-A".to_string(), tier);
            guard.tiers.insert("COLD-B".to_string(), build_rustfs_tier("COLD-B"));
            guard
                .replace_driver("COLD-A", Box::new(LeaseTestBackend::ready("cleanup")))
                .expect("cleanup driver generation should install");
            guard
                .replace_driver("COLD-B", Box::new(LeaseTestBackend::ready("stale-local")))
                .expect("stale local driver generation should install");
        }
        let cleanup_lease = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("in-flight cleanup lease should be available");
        let stale_local_lease = TierConfigMgr::acquire_operation_lease(&manager, "COLD-B")
            .await
            .expect("stale local tier lease should be available");
        let mut free_version = transitioned_tier_object("photos", "2026/free-version.jpg", "COLD-A", Some(identity));
        free_version.transitioned_object.status = "pending".to_string();
        free_version.transitioned_object.free_version = true;
        store.add_listed_version(free_version);

        let remove_manager = manager.clone();
        let remove_store = store.clone();
        let remove = tokio::spawn(async move {
            TIER_MUTATION_TEST_PEERS
                .scope(
                    Vec::new(),
                    TierConfigMgr::remove_and_save_with(&remove_manager, remove_store, "COLD-A", true),
                )
                .await
        });

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let guard = manager.read().await;
                let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
                let prepared = {
                    let runtime = lock_unpoisoned(&runtime);
                    runtime.prepared_mutation_blocks.contains_key("COLD-A")
                        && runtime.prepared_mutation_blocks.contains_key("COLD-B")
                };
                if prepared {
                    break;
                }
                drop(guard);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("tier remove should install its durable prepared fence");

        assert!(
            cleanup_lease.is_current(&manager).await,
            "the prepared fence must let the already leased cleanup finish its exact local marker deletion"
        );
        assert!(
            stale_local_lease.is_current(&manager).await,
            "the local superset fence must also let an already leased stale-manager operation finish"
        );
        let blocked = match TierConfigMgr::acquire_operation_lease(&manager, "COLD-A").await {
            Ok(_) => panic!("the prepared fence must reject new tier operations"),
            Err(err) => err,
        };
        assert!(TierConfigMgr::operation_lease_blocked_by_mutation(&blocked));
        let stale_blocked = match TierConfigMgr::acquire_operation_lease(&manager, "COLD-B").await {
            Ok(_) => panic!("the local superset fence must reject new stale-manager operations"),
            Err(err) => err,
        };
        assert!(TierConfigMgr::operation_lease_blocked_by_mutation(&stale_blocked));

        store.remove_listed_version("photos", "2026/free-version.jpg");
        drop(cleanup_lease);
        drop(stale_local_lease);

        tokio::time::timeout(Duration::from_secs(5), remove)
            .await
            .expect("tier remove should finish after both in-flight operations release their leases")
            .expect("tier remove task should join")
            .expect("tier remove should pass once the in-flight cleanup removes its marker");
        assert!(!manager.read().await.tiers.contains_key("COLD-A"));
        assert!(!manager.read().await.tiers.contains_key("COLD-B"));
        assert!(
            !load_tier_config_for_update(store)
                .await
                .expect("removed tier config should reload")
                .0
                .tiers
                .contains_key("COLD-A")
        );
    }

    #[tokio::test]
    async fn no_intent_stale_manager_removal_keeps_early_generation_drain() {
        let store = Arc::new(CasConfigStore::default());
        empty_mgr()
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("empty persisted tier config should exist");
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("stale"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("stale manager lease should be available");

        let remove_manager = manager.clone();
        let remove_store = store.clone();
        let remove =
            tokio::spawn(async move { TierConfigMgr::remove_and_save_with(&remove_manager, remove_store, "COLD-A", true).await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while old.is_current(&manager).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("no-intent stale-manager reconciliation should revoke before its proof");
        let blocked = match TierConfigMgr::acquire_operation_lease(&manager, "COLD-A").await {
            Ok(_) => panic!("stale-manager reconciliation must not admit a new operation"),
            Err(err) => err,
        };
        assert!(TierConfigMgr::operation_lease_blocked_by_mutation(&blocked));

        drop(old);
        remove
            .await
            .expect("stale-manager removal task should join")
            .expect("stale-manager removal should converge to the persisted empty config");
        assert!(!manager.read().await.tiers.contains_key("COLD-A"));
    }

    async fn assert_lifecycle_only_reference_obeys_force(clear: bool, force: bool) {
        let store = Arc::new(CasConfigStore::default());
        let tier = build_rustfs_tier("COLD-A");
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), tier.clone_with_credentials());
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("reference proof fixture should persist");
        let config = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("move-current".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: Some(vec![Transition {
                    days: Some(1),
                    date: None,
                    storage_class: Some(TransitionStorageClass::from_static("COLD-A")),
                }]),
            }],
        };
        // CasConfigStore::list_bucket derives its fake bucket listing from listed_versions, so
        // a bucket with a lifecycle rule but zero objects still needs a decoy entry to be
        // visible to the reference-proof walk at all (mirrors production, where list_bucket
        // enumerates real buckets independent of their contents).
        store.add_listed_version(ObjectInfo {
            bucket: "photos".to_string(),
            name: "safe.txt".to_string(),
            ..Default::default()
        });
        store.add_lifecycle_config("photos", config);

        let manager = TierConfigMgr::new();
        manager.write().await.tiers.insert("COLD-A".to_string(), tier);
        let mutation = if clear {
            TierCandidateMutation::Clear(force)
        } else {
            TierCandidateMutation::Remove("COLD-A".to_string(), force)
        };
        let result = TIER_DRIVER_TEST_FACTORY
            .scope(
                healthy_driver_factory(),
                TierConfigMgr::update_candidate_with_config_lock(&manager, store.clone(), mutation),
            )
            .await;
        if force {
            result.expect("force mutation must bypass a lifecycle-config-only reference");
        } else {
            let err = result.expect_err("non-force mutation must reject a lifecycle-only reference");
            let TierConfigUpdateError::Publish(err) = err else {
                panic!("non-force mutation must fail during reference proof: {err:?}");
            };
            assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
            assert!(err.message.contains("move-current"), "{err}");
        }

        assert_eq!(manager.read().await.tiers.contains_key("COLD-A"), !force);
        assert_eq!(
            load_tier_config_for_update(store)
                .await
                .expect("config should still reload")
                .0
                .tiers
                .contains_key("COLD-A"),
            !force,
            "persisted state must match the force mutation result"
        );
    }

    #[tokio::test]
    async fn remove_with_config_lock_obeys_force_for_lifecycle_only_reference() {
        for force in [false, true] {
            assert_lifecycle_only_reference_obeys_force(false, force).await;
        }
    }

    #[tokio::test]
    async fn clear_with_config_lock_obeys_force_for_lifecycle_only_reference() {
        for force in [false, true] {
            assert_lifecycle_only_reference_obeys_force(true, force).await;
        }
    }

    #[tokio::test]
    async fn zero_reference_proof_blocks_clear_before_config_save() {
        let store = Arc::new(CasConfigStore::default());
        let tier_a = build_rustfs_tier("COLD-A");
        let tier_b = build_rustfs_tier("COLD-B");
        let identity_b = tier_backend_identity(&tier_b).expect("test tier identity should encode");
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), tier_a.clone_with_credentials());
        persisted.tiers.insert("COLD-B".to_string(), tier_b.clone_with_credentials());
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("reference proof fixture should persist");
        store.add_listed_version(transitioned_tier_object("photos", "2026/b.jpg", "COLD-B", Some(identity_b)));

        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            guard.tiers.insert("COLD-A".to_string(), tier_a);
            guard.tiers.insert("COLD-B".to_string(), tier_b);
        }
        let err = TierConfigMgr::clear_and_save_with(&manager, store.clone(), true)
            .await
            .expect_err("authoritative object reference must block tier clear");

        match err {
            TierConfigUpdateError::Publish(err) => {
                assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
                assert!(err.message.contains("photos/2026/b.jpg"), "{}", err.message);
            }
            other => panic!("reference proof failures must be surfaced as publish errors: {other:?}"),
        }
        let guard = manager.read().await;
        assert!(guard.tiers.contains_key("COLD-A"));
        assert!(guard.tiers.contains_key("COLD-B"));
        drop(guard);
        let reloaded = load_tier_config_for_update(store)
            .await
            .expect("blocked config should still reload")
            .0;
        assert!(reloaded.tiers.contains_key("COLD-A"));
        assert!(reloaded.tiers.contains_key("COLD-B"));
    }

    #[tokio::test]
    async fn zero_reference_proof_rebind_allows_only_new_destination_references() {
        let current = build_rustfs_tier("COLD-A");
        let current_identity = tier_backend_identity(&current).expect("current identity should encode");
        let mut replacement = build_rustfs_tier("COLD-A");
        replacement.rustfs.as_mut().expect("replacement payload should exist").prefix = "new-prefix".to_string();
        let replacement_identity = tier_backend_identity(&replacement).expect("replacement identity should encode");
        assert_ne!(current_identity, replacement_identity);
        let target = TierMutationIntentTarget {
            tier_name: "COLD-A".to_string(),
            old_backend_identity: Some(current_identity),
            new_backend_identity: Some(replacement_identity),
        };

        let store = Arc::new(CasConfigStore::default());
        store.add_listed_version(transitioned_tier_object(
            "photos",
            "2026/new-destination.jpg",
            "COLD-A",
            Some(replacement_identity),
        ));
        ensure_no_authoritative_tier_object_references(store.clone(), std::slice::from_ref(&target), false)
            .await
            .expect("references already written with the new destination identity should not block rebind");

        store.add_listed_version(transitioned_tier_object(
            "photos",
            "2026/old-destination.jpg",
            "COLD-A",
            Some(current_identity),
        ));
        let err = ensure_no_authoritative_tier_object_references(store.clone(), &[target], false)
            .await
            .expect_err("references to the old destination identity must block rebind");
        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(err.message.contains("photos/2026/old-destination.jpg"), "{}", err.message);
    }

    #[tokio::test]
    async fn zero_reference_proof_blocks_lifecycle_transition_references() {
        let current = build_rustfs_tier("COLD-A");
        let current_identity = tier_backend_identity(&current).expect("current identity should encode");
        let target = TierMutationIntentTarget {
            tier_name: "COLD-A".to_string(),
            old_backend_identity: Some(current_identity),
            new_backend_identity: None,
        };
        let config = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("move-current".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: Some(vec![Transition {
                    days: Some(1),
                    date: None,
                    storage_class: Some(TransitionStorageClass::from_static("COLD-A")),
                }]),
            }],
        };
        let store = Arc::new(CasConfigStore::default());
        store.add_listed_version(ObjectInfo {
            bucket: "photos".to_string(),
            name: "safe.txt".to_string(),
            ..Default::default()
        });
        store.add_lifecycle_config("photos", config);

        let err = ensure_no_authoritative_tier_object_references(store, &[target], false)
            .await
            .expect_err("lifecycle rule should block deletion");

        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(err.message.contains("move-current"), "{}", err.message);
        assert!(err.message.contains("photos"), "{}", err.message);
    }

    #[tokio::test]
    async fn zero_reference_proof_blocks_lifecycle_noncurrent_transition_references() {
        let current = build_rustfs_tier("COLD-A");
        let current_identity = tier_backend_identity(&current).expect("current identity should encode");
        let target = TierMutationIntentTarget {
            tier_name: "COLD-A".to_string(),
            old_backend_identity: Some(current_identity),
            new_backend_identity: None,
        };
        let config = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("move-noncurrent".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: Some(vec![NoncurrentVersionTransition {
                    noncurrent_days: Some(1),
                    newer_noncurrent_versions: None,
                    storage_class: Some(TransitionStorageClass::from_static("COLD-A")),
                }]),
                prefix: None,
                transitions: None,
            }],
        };
        let store = Arc::new(CasConfigStore::default());
        store.add_listed_version(ObjectInfo {
            bucket: "photos".to_string(),
            name: "safe.txt".to_string(),
            ..Default::default()
        });
        store.add_lifecycle_config("photos", config);

        let err = ensure_no_authoritative_tier_object_references(store, &[target], false)
            .await
            .expect_err("lifecycle rule should block deletion");

        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(err.message.contains("move-noncurrent"), "{}", err.message);
        assert!(err.message.contains("photos"), "{}", err.message);
    }

    #[tokio::test]
    async fn zero_reference_proof_force_true_skips_lifecycle_reference_check() {
        // rustfs/rustfs#6832: force=true must bypass a lifecycle-config-only reference,
        // mirroring the existing `!force` gate on `TierConfigMgr::remove()`'s own
        // `driver.in_use()` probe. It must NOT bypass real object/journal/transaction
        // references — those stay force-immune and are covered separately below.
        let current = build_rustfs_tier("COLD-A");
        let current_identity = tier_backend_identity(&current).expect("current identity should encode");
        let target = TierMutationIntentTarget {
            tier_name: "COLD-A".to_string(),
            old_backend_identity: Some(current_identity),
            new_backend_identity: None,
        };
        let config = BucketLifecycleConfiguration {
            expiry_updated_at: None,
            rules: vec![LifecycleRule {
                status: ExpirationStatus::from_static(ExpirationStatus::ENABLED),
                expiration: None,
                abort_incomplete_multipart_upload: None,
                del_marker_expiration: None,
                filter: None,
                id: Some("move-current".to_string()),
                noncurrent_version_expiration: None,
                noncurrent_version_transitions: None,
                prefix: None,
                transitions: Some(vec![Transition {
                    days: Some(1),
                    date: None,
                    storage_class: Some(TransitionStorageClass::from_static("COLD-A")),
                }]),
            }],
        };
        let store = Arc::new(CasConfigStore::default());
        store.add_listed_version(ObjectInfo {
            bucket: "photos".to_string(),
            name: "safe.txt".to_string(),
            ..Default::default()
        });
        store.add_lifecycle_config("photos", config);

        ensure_no_authoritative_tier_object_references(store, &[target], true)
            .await
            .expect("force=true must bypass a lifecycle-config-only reference");
    }

    #[tokio::test]
    async fn zero_reference_proof_blocks_persisted_journal_transaction_and_free_version_references() {
        // All three sub-checks below pass `force: true` to pin that physical/persisted
        // references stay force-immune post rustfs/rustfs#6832 (only the lifecycle-config
        // check, covered by `zero_reference_proof_force_true_skips_lifecycle_reference_check`
        // above, is meant to be force-bypassable).
        let current = build_rustfs_tier("COLD-A");
        let current_identity = tier_backend_identity(&current).expect("current identity should encode");
        let replacement = build_azure_tier("account-a");
        let replacement_identity = tier_backend_identity(&replacement).expect("replacement identity should encode");
        let target = TierMutationIntentTarget {
            tier_name: "COLD-A".to_string(),
            old_backend_identity: Some(current_identity),
            new_backend_identity: Some(replacement_identity),
        };

        let journal_store = Arc::new(CasConfigStore::default());
        let journal = crate::bucket::lifecycle::tier_sweeper::Jentry {
            persisted_version: 0,
            obj_name: "remote/journal-object".to_string(),
            version_id: "v1".to_string(),
            tier_name: "COLD-A".to_string(),
            backend_identity: Some(current_identity),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState::Committed,
            source: None,
            dispatch: None,
        };
        journal_store
            .insert_config_object(
                crate::bucket::lifecycle::tier_delete_journal::tier_delete_journal_object_name(&journal),
                crate::bucket::lifecycle::tier_delete_journal::encode_tier_delete_journal_entry(&journal)
                    .expect("journal record should encode"),
            )
            .await;
        let err = ensure_no_authoritative_tier_object_references(journal_store, std::slice::from_ref(&target), true)
            .await
            .expect_err("unfinished delete journal for the old backend must block rebind even with force");
        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(err.message.contains(TIER_DELETE_JOURNAL_PREFIX), "{}", err.message);

        let transaction_store = Arc::new(CasConfigStore::default());
        let transaction = crate::bucket::lifecycle::transition_transaction::TransitionTransaction::new(
            crate::bucket::lifecycle::transition_transaction::TransitionTransactionInit {
                deployment_id: uuid::Uuid::new_v4(),
                transaction_id: uuid::Uuid::new_v4(),
                owner_epoch: uuid::Uuid::new_v4(),
                write_id: uuid::Uuid::new_v4(),
                source: crate::bucket::lifecycle::transition_transaction::TransitionSourceIdentity {
                    bucket: "photos".to_string(),
                    object: "2026/transaction.jpg".to_string(),
                    version_id: None,
                    data_dir: uuid::Uuid::new_v4(),
                    mod_time_unix_nanos: 1,
                    size: 1,
                    etag: "etag".to_string(),
                    version_mode: crate::bucket::lifecycle::transition_transaction::TransitionSourceVersionMode::Unversioned,
                },
                tier_name: "COLD-A".to_string(),
                backend_fingerprint: current_identity,
                not_after_unix_nanos: 2,
            },
        )
        .expect("transaction record should initialize");
        transaction_store
            .insert_config_object(
                crate::bucket::lifecycle::transition_transaction::transition_transaction_record_object_name(
                    transaction.transaction_id,
                )
                .expect("transaction record path should build"),
                transaction.encode().expect("transaction record should encode"),
            )
            .await;
        let err = ensure_no_authoritative_tier_object_references(transaction_store, std::slice::from_ref(&target), true)
            .await
            .expect_err("unfinished transition transaction for the old backend must block rebind even with force");
        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(err.message.contains(TRANSITION_TRANSACTION_RECORD_PREFIX), "{}", err.message);

        let free_version_store = Arc::new(CasConfigStore::default());
        let mut free_version = transitioned_tier_object("photos", "2026/free-version.jpg", "COLD-A", Some(current_identity));
        free_version.transitioned_object.status = "pending".to_string();
        free_version.transitioned_object.free_version = true;
        ensure_no_authoritative_tier_object_references(free_version_store.clone(), std::slice::from_ref(&target), true)
            .await
            .expect("empty free-version fixture should permit rebind");
        free_version_store.add_listed_version(free_version);
        let err = ensure_no_authoritative_tier_object_references(free_version_store, &[target], true)
            .await
            .expect_err("recoverable free version for the old backend must block rebind even with force");
        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
        assert!(err.message.contains("photos/2026/free-version.jpg"), "{}", err.message);
    }

    #[tokio::test]
    async fn zero_reference_proof_fails_closed_when_internal_walk_fails() {
        let current = build_rustfs_tier("COLD-A");
        let current_identity = tier_backend_identity(&current).expect("current identity should encode");
        let target = TierMutationIntentTarget {
            tier_name: "COLD-A".to_string(),
            old_backend_identity: Some(current_identity),
            new_backend_identity: None,
        };
        let store = Arc::new(CasConfigStore::default());
        store.add_listed_version(ObjectInfo {
            bucket: "photos".to_string(),
            name: "safe/object.jpg".to_string(),
            ..Default::default()
        });
        for index in 0..500 {
            store.add_listed_version(ObjectInfo {
                bucket: "photos".to_string(),
                name: format!("safe/trailing-{index:04}.jpg"),
                ..Default::default()
            });
        }
        store.fail_reference_walk();

        let err = ensure_no_authoritative_tier_object_references(store.clone(), &[target], false)
            .await
            .expect_err("authoritative internal walk failure must fail closed");
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert!(
            err.message.contains("injected tier reference walk failure"),
            "unexpected reference proof error: {}",
            err.message
        );
        assert!(
            store.reference_walk_send_count() <= 100,
            "the first terminal walk error may overshoot only by the bounded channel capacity"
        );
    }

    #[test]
    fn zero_reference_proof_rebind_identity_boundaries_fail_closed() {
        let tier = build_rustfs_tier("COLD-A");
        let current_identity = tier_backend_identity(&tier).expect("current identity should encode");
        let replacement_identity =
            tier_backend_identity(&build_azure_tier("account-a")).expect("replacement identity should encode");
        let target = TierMutationIntentTarget {
            tier_name: "COLD-A".to_string(),
            old_backend_identity: Some(current_identity),
            new_backend_identity: Some(current_identity),
        };
        let object = transitioned_tier_object("photos", "2026/a.jpg", "COLD-A", Some(current_identity));
        assert!(!tier_object_blocks_target_rebind(&object, &target).expect("matching identity should parse"));

        let target = TierMutationIntentTarget {
            tier_name: "COLD-A".to_string(),
            old_backend_identity: Some(current_identity),
            new_backend_identity: Some(replacement_identity),
        };
        assert!(tier_object_blocks_target_rebind(&object, &target).expect("mismatched identity should parse"));

        let object_without_identity = transitioned_tier_object("photos", "2026/b.jpg", "COLD-A", None);
        assert!(
            tier_object_blocks_target_rebind(&object_without_identity, &target)
                .expect("missing identity means unknown reference owner")
        );

        let mut pending = transitioned_tier_object("photos", "2026/c.jpg", "COLD-A", Some(current_identity));
        pending.transitioned_object.status = "pending".to_string();
        assert!(!tier_object_blocks_target_rebind(&pending, &target).expect("non-complete status should not block"));
    }

    #[tokio::test]
    async fn mutation_target_proof_uses_persisted_config_snapshot_not_stale_manager() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        let mut candidate = empty_mgr();
        candidate.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        candidate.tiers.insert("COLD-B".to_string(), build_rustfs_tier("COLD-B"));
        candidate
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("stale-manager fixture should persist");
        let (candidate, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("stale-manager fixture should reload with an ETag");
        let update = TierConfigMgr::admin_update_lock(&manager).await;

        TierConfigMgr::update_candidate_owned(
            &manager,
            store.clone(),
            candidate,
            version,
            TierCandidateMutation::Remove("COLD-A".to_string(), true),
            update,
            None,
        )
        .await
        .expect("authoritative proof should use the loaded config even when the local manager is stale");
        assert!(!manager.read().await.tiers.contains_key("COLD-A"));
        assert!(manager.read().await.tiers.contains_key("COLD-B"));
        let reloaded = load_tier_config_for_update(store)
            .await
            .expect("removed config should reload")
            .0;
        assert!(!reloaded.tiers.contains_key("COLD-A"));
        assert!(reloaded.tiers.contains_key("COLD-B"));
    }

    #[tokio::test]
    async fn legacy_config_without_etag_blocks_non_add_mutations_before_save() {
        for (case, mutation) in [
            (
                "edit",
                TierCandidateMutation::Edit(
                    "COLD-A".to_string(),
                    TierCreds {
                        access_key: "rotated-access".to_string(),
                        secret_key: "rotated-secret".to_string(),
                        ..Default::default()
                    },
                ),
            ),
            ("remove", TierCandidateMutation::Remove("COLD-A".to_string(), true)),
            ("clear", TierCandidateMutation::Clear(true)),
        ] {
            let manager = TierConfigMgr::new();
            let store = Arc::new(CasConfigStore::default());
            let mut legacy = empty_mgr();
            legacy.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
            let legacy_data = legacy.marshal().expect("legacy tier config should encode");
            *store.legacy_state.lock().await = Some(legacy_data.to_vec());

            let (loaded, version) = load_tier_config_for_update(store.clone())
                .await
                .expect("legacy tier config should load for update");
            assert!(loaded.tiers.contains_key("COLD-A"), "{case} fixture should load legacy config");
            assert_eq!(version, None, "{case} fixture must represent a legacy config without current ETag");

            let err = TierConfigMgr::update_candidate_with_config_lock(&manager, store.clone(), mutation)
                .await
                .expect_err("non-add legacy config mutations must fail closed before save");

            match err {
                TierConfigUpdateError::Load(err) => {
                    assert!(
                        err.to_string().contains("requires an existing config ETag"),
                        "{case} returned unexpected load error: {err}"
                    );
                }
                other => panic!("{case} should fail before mutation/save, got {other:?}"),
            }
            assert!(
                !store.objects.lock().await.contains_key(&tier_config_path(TIER_CONFIG_FILE)),
                "{case} must not create the durable binary tier config after a failed legacy mutation"
            );
            assert!(
                manager.read().await.tiers.is_empty(),
                "{case} must not publish a failed legacy mutation into the live manager"
            );
        }
    }

    #[test]
    fn add_target_proof_ignores_unchanged_persisted_tiers_when_manager_is_stale() {
        let mut current = empty_mgr();
        current.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));

        let mut candidate = empty_mgr();
        candidate.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        candidate.tiers.insert("COLD-B".to_string(), build_rustfs_tier("COLD-B"));

        let targets = TierCandidateMutation::Add(Box::new(build_rustfs_tier("COLD-B")), true)
            .affected_targets(&current, &candidate)
            .expect("add proof should ignore unchanged durable tiers");
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].tier_name, "COLD-B");
        assert!(targets[0].old_backend_identity.is_none());
        assert!(targets[0].new_backend_identity.is_some());
    }

    #[tokio::test]
    async fn update_with_config_lock_add_uses_loaded_snapshot_for_target_proof() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("add fixture should persist");

        TIER_DRIVER_TEST_FACTORY
            .scope(
                healthy_driver_factory(),
                TierConfigMgr::update_candidate_with_config_lock(
                    &manager,
                    store.clone(),
                    TierCandidateMutation::Add(Box::new(build_rustfs_tier("COLD-B")), true),
                ),
            )
            .await
            .expect("add proof must ignore unchanged durable tiers missing from the stale manager");

        let reloaded = load_tier_config_for_update(store)
            .await
            .expect("updated config should reload")
            .0;
        assert!(reloaded.tiers.contains_key("COLD-A"));
        assert!(reloaded.tiers.contains_key("COLD-B"));
    }

    #[tokio::test]
    async fn nested_only_legacy_add_builds_nonempty_coordinator_target_and_fans_out() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        empty_mgr()
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("empty tier config fixture should persist");
        let legacy_wire: TierConfig = serde_json::from_value(serde_json::json!({
            "type": "s3",
            "s3": {
                "name": "COLD-LEGACY",
                "endpoint": "https://s3.example.invalid",
                "accessKey": "access",
                "secretKey": "secret",
                "bucket": "archive",
                "prefix": "objects",
                "region": "us-east-1",
                "storageClass": "STANDARD"
            }
        }))
        .expect("legacy RustFS nested-name AddTier payload should decode");
        assert!(legacy_wire.name.is_empty());
        let mutation = TierCandidateMutation::add(legacy_wire, true)
            .expect("legacy nested name must normalize before constructing the Add mutation");
        assert_eq!(mutation.explicit_tier_name(), Some("COLD-LEGACY"));

        let peer_calls = Arc::new(Mutex::new(Vec::new()));
        let prepared_intents = Arc::new(Mutex::new(Vec::new()));
        let backend = RecordingWarmBackend::new();
        TIER_DRIVER_TEST_FACTORY
            .scope(
                recording_driver_factory(backend, Arc::new(Mutex::new(Vec::new()))),
                TIER_MUTATION_TEST_PEERS.scope(
                    vec![FakeTierMutationPeer::boxed_capturing_prepare(
                        "peer-a",
                        peer_calls.clone(),
                        prepared_intents.clone(),
                    )],
                    TierConfigMgr::update_candidate_with_config_lock(&manager, store.clone(), mutation),
                ),
            )
            .await
            .expect("legacy nested-name Add must run the full coordinator fanout");

        {
            let prepared_intents = lock_unpoisoned(&prepared_intents);
            assert_eq!(prepared_intents.len(), 1);
            assert_eq!(prepared_intents[0].kind, TierMutationIntentKind::Add);
            assert_eq!(prepared_intents[0].affected_targets.len(), 1);
            assert_eq!(prepared_intents[0].affected_targets[0].tier_name, "COLD-LEGACY");
            assert!(prepared_intents[0].affected_targets[0].old_backend_identity.is_none());
            assert!(prepared_intents[0].affected_targets[0].new_backend_identity.is_some());
        }

        let peer_calls = lock_unpoisoned(&peer_calls).clone();
        let prepare_index = peer_calls
            .iter()
            .position(|call| call.starts_with("peer-a:prepare:"))
            .expect("legacy Add should prepare the peer");
        let commit_index = peer_calls
            .iter()
            .position(|call| call.starts_with("peer-a:commit:"))
            .expect("legacy Add should commit the peer");
        assert!(prepare_index < commit_index, "{peer_calls:?}");

        let reloaded = load_tier_config_for_update(store)
            .await
            .expect("legacy Add result should reload")
            .0;
        let persisted = reloaded
            .tiers
            .get("COLD-LEGACY")
            .expect("normalized tier name should be persisted");
        assert_eq!(persisted.name, "COLD-LEGACY");
        assert_eq!(persisted.s3.as_ref().expect("persisted S3 payload should exist").name, "COLD-LEGACY");
        assert!(manager.read().await.tiers.contains_key("COLD-LEGACY"));
    }

    #[tokio::test]
    async fn config_update_refuses_pending_prepared_recovery_before_advancing_etag() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("base tier config should persist");
        let (loaded, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("base tier config should load with metadata");
        let base_etag = version.expect("base tier config should carry an ETag");

        let mut candidate = TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: loaded
                .tiers
                .iter()
                .map(|(tier_name, config)| (tier_name.clone(), config.clone_with_credentials()))
                .collect(),
            last_refreshed_at: loaded.last_refreshed_at,
        };
        candidate
            .tiers
            .get_mut("COLD-A")
            .expect("COLD-A should exist")
            .rustfs
            .as_mut()
            .expect("COLD-A RustFS payload should exist")
            .secret_key = "rotated-secret".to_string();
        let affected_targets = TierCandidateMutation::Edit(
            "COLD-A".to_string(),
            TierCreds {
                secret_key: "rotated-secret".to_string(),
                ..Default::default()
            },
        )
        .affected_targets(&loaded, &candidate)
        .expect("prepared edit targets should build");
        let prepared = build_coordinator_tier_mutation_intent(
            TierMutationIntentKind::Edit,
            Some(base_etag.clone()),
            &candidate,
            affected_targets,
        )
        .expect("prepared coordinator intent should build")
        .expect("prepared coordinator intent should be required");
        save_coordinator_tier_mutation_intent(store.clone(), Some(&prepared))
            .await
            .expect("prepared coordinator intent should persist");

        let err = TierConfigMgr::update_candidate_with_config_lock(
            &manager,
            store.clone(),
            TierCandidateMutation::Add(Box::new(build_rustfs_tier("COLD-B")), true),
        )
        .await
        .expect_err("a new tier config update must wait for pending mutation recovery");
        let TierConfigUpdateError::Publish(err) = err else {
            panic!("pending mutation recovery should report a publish conflict");
        };
        assert!(
            err.message.contains("mutation recovery must finish"),
            "unexpected pending-recovery error: {}",
            err.message
        );

        let (unchanged, current_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("tier config should remain readable after rejected update");
        assert_eq!(current_etag.as_deref(), Some(base_etag.as_str()));
        assert!(unchanged.tiers.contains_key("COLD-A"));
        assert!(
            !unchanged.tiers.contains_key("COLD-B"),
            "the unrelated update must not advance the config generation"
        );

        let intents = TierConfigMgr::load_coordinator_mutation_intents(store.clone())
            .await
            .expect("coordinator intent should remain recoverable");
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].state, TierMutationIntentState::Prepared);

        TierConfigMgr::reload_handle_with(&manager, store.clone())
            .await
            .expect("reload should retain the unexpired prepared owner");
        let intents = TierConfigMgr::load_coordinator_mutation_intents(store.clone())
            .await
            .expect("unexpired coordinator intent should remain recoverable");
        assert_eq!(intents.len(), 1);
        assert_eq!(intents[0].state, TierMutationIntentState::Prepared);

        store.expire_coordinator_intent(prepared).await;
        TierConfigMgr::reload_handle_with(&manager, store.clone())
            .await
            .expect("reload should clean the expired pending recovery");
        assert!(
            TierConfigMgr::load_coordinator_mutation_intents(store)
                .await
                .expect("coordinator intent scan should succeed after cleanup")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn failed_owned_update_restores_generation_and_reports_save_error() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let store = Arc::new(CasConfigStore::default());
        let mut candidate = empty_mgr();
        candidate.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        candidate
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("save-failure fixture should persist");
        let (_, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("save-failure fixture should reload with an ETag");
        store.fail_put.store(true, Ordering::SeqCst);
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("candidate")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;

        let err = TierConfigMgr::update_candidate_owned(
            &manager,
            store.clone(),
            candidate,
            version.clone(),
            TierCandidateMutation::Remove("COLD-A".to_string(), true),
            update,
            None,
        )
        .await
        .expect_err("save failure must be observable to the admin caller");
        assert!(matches!(err, TierConfigUpdateError::Save(_)));
        assert!(manager.read().await.tiers.contains_key("COLD-A"));
        let restored = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("the old generation must be restored after save failure");
        assert!(restored.is_current(&manager).await);
        drop(restored);
        let guard = manager.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
        assert!(!lock_unpoisoned(&runtime).draining.contains_key("COLD-A"));
        drop(guard);

        store.fail_put.store(false, Ordering::SeqCst);
        let mut retry = empty_mgr();
        retry.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        retry
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("retry")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;
        TierConfigMgr::update_candidate_owned(
            &manager,
            store,
            retry,
            version,
            TierCandidateMutation::Remove("COLD-A".to_string(), true),
            update,
            None,
        )
        .await
        .expect("a later update must succeed after save failure recovery");
        assert!(!manager.read().await.tiers.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn cancelled_owned_update_finishes_after_lease_drain() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old generation lease should be available");
        let store = Arc::new(CasConfigStore::default());
        let mut candidate = empty_mgr();
        candidate.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        candidate
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("cancelled update fixture should persist");
        let (_, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("cancelled update fixture should reload with an ETag");
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("candidate")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;
        let update_manager = manager.clone();
        let update_store = store.clone();
        let caller = tokio::spawn(async move {
            TierConfigMgr::update_candidate_owned(
                &update_manager,
                update_store,
                candidate,
                version,
                TierCandidateMutation::Remove("COLD-A".to_string(), true),
                update,
                None,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let guard = manager.read().await;
                let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
                let prepared = lock_unpoisoned(&runtime).prepared_mutation_blocks.contains_key("COLD-A");
                if prepared {
                    break;
                }
                drop(guard);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned update should install its prepared fence before caller cancellation");
        assert!(
            old.is_current(&manager).await,
            "an already leased operation must remain current until it can finish"
        );
        caller.abort();
        drop(old);

        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.read().await.tiers.contains_key("COLD-A") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached update must finish after its operation lease drains");
        let guard = manager.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
        assert!(!lock_unpoisoned(&runtime).draining.contains_key("COLD-A"));
        assert!(!store.objects.lock().await.is_empty(), "detached update must persist its result");
    }

    #[tokio::test]
    async fn config_write_lock_is_released_while_cancelled_update_drains_leases() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let old = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("old generation lease should be available");
        let store = Arc::new(CasConfigStore::default());
        let mut candidate = empty_mgr();
        candidate.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        candidate
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("tier config fixture should persist");
        let update_manager = manager.clone();
        let update_store = store.clone();
        let caller = tokio::spawn(async move {
            TierConfigMgr::update_candidate_with_config_lock(
                &update_manager,
                update_store,
                TierCandidateMutation::Remove("COLD-A".to_string(), true),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let guard = manager.read().await;
                let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
                let prepared = lock_unpoisoned(&runtime).prepared_mutation_blocks.contains_key("COLD-A");
                if prepared {
                    break;
                }
                drop(guard);
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned update should install its prepared fence before caller cancellation");
        assert!(
            old.is_current(&manager).await,
            "the prepared fence must not invalidate an already leased operation"
        );
        caller.abort();

        let config_file = tier_config_lock_path();
        let competing_lock = store
            .new_ns_lock(RUSTFS_META_BUCKET, &config_file)
            .await
            .expect("competing tier config lock should be created");
        let competing_guard = competing_lock
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("durable Prepared must allow the config lock to release while leases drain");
        drop(competing_guard);

        drop(old);
        tokio::time::timeout(Duration::from_secs(1), async {
            while manager.read().await.tiers.contains_key("COLD-A") {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached update must finish after its operation lease drains");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn backend_validation_runs_without_holding_the_config_namespace_lock() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        empty_mgr()
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("backend-validation fixture should persist");
        let (barrier, _barrier_guard) = install_tier_driver_build_barrier("COLD-A");
        let update_manager = manager.clone();
        let update_store = store.clone();
        let update = tokio::spawn(async move {
            TIER_DRIVER_TEST_FACTORY
                .scope(
                    healthy_driver_factory(),
                    TierConfigMgr::update_candidate_with_config_lock(
                        &update_manager,
                        update_store,
                        TierCandidateMutation::Add(Box::new(build_rustfs_tier("COLD-A")), true),
                    ),
                )
                .await
        });
        barrier.arrived.notified().await;

        let competing_lock = store
            .new_ns_lock(RUSTFS_META_BUCKET, &tier_config_lock_path())
            .await
            .expect("competing config lock should be created");
        let competing_guard = competing_lock
            .get_write_lock(Duration::from_millis(100))
            .await
            .expect("remote backend validation must not hold the config namespace lock");
        drop(competing_guard);

        barrier.release.add_permits(1);
        update
            .await
            .expect("backend-validation update task should join")
            .expect("backend-validation update should succeed");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn backend_validation_rejects_a_changed_persisted_etag_before_prepare() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        empty_mgr()
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("stale backend-validation fixture should persist");
        let peer_calls = Arc::new(Mutex::new(Vec::new()));
        let (barrier, _barrier_guard) = install_tier_driver_build_barrier("COLD-A");
        let update_manager = manager.clone();
        let update_store = store.clone();
        let update_peer_calls = peer_calls.clone();
        let update = tokio::spawn(async move {
            TIER_DRIVER_TEST_FACTORY
                .scope(
                    healthy_driver_factory(),
                    TIER_MUTATION_TEST_PEERS.scope(
                        vec![FakeTierMutationPeer::boxed(
                            "peer-a",
                            update_peer_calls,
                            Ok(PeerTierMutationState::Prepared),
                        )],
                        TierConfigMgr::update_candidate_with_config_lock(
                            &update_manager,
                            update_store,
                            TierCandidateMutation::Add(Box::new(build_rustfs_tier("COLD-A")), true),
                        ),
                    ),
                )
                .await
        });
        barrier.arrived.notified().await;

        let config_lock = store
            .new_ns_lock(RUSTFS_META_BUCKET, &tier_config_lock_path())
            .await
            .expect("competing config lock should be created");
        let config_guard = config_lock
            .get_write_lock(Duration::from_millis(100))
            .await
            .expect("backend validation should leave the persisted generation available");
        let (mut competing, competing_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("competing config should load");
        competing.tiers.insert("COLD-B".to_string(), build_rustfs_tier("COLD-B"));
        competing
            .save_tiering_config_if_current(store.clone(), competing_etag.as_deref())
            .await
            .expect("competing config generation should commit");
        drop(config_guard);
        barrier.release.add_permits(1);

        let err = update
            .await
            .expect("stale backend-validation update task should join")
            .expect_err("a changed persisted ETag must reject the prevalidated candidate");
        let TierConfigUpdateError::Load(err) = err else {
            panic!("stale backend validation should report a load conflict");
        };
        assert!(
            err.to_string()
                .contains("changed while the remote backend mutation was being validated")
        );
        let current = load_tier_config_for_update(store.clone())
            .await
            .expect("winning config should reload")
            .0;
        assert!(current.tiers.contains_key("COLD-B"));
        assert!(!current.tiers.contains_key("COLD-A"));
        assert!(lock_unpoisoned(&peer_calls).is_empty(), "stale validation must not send peer Prepare");
        assert!(
            TierConfigMgr::load_coordinator_mutation_intents(store.clone())
                .await
                .expect("stale validation intent scan should succeed")
                .is_empty(),
            "stale validation must not leave a coordinator Prepared intent"
        );
    }

    #[tokio::test]
    async fn detached_backend_validation_panic_is_reported_as_mutation_failure() {
        let panic_factory: TierDriverTestFactory = Arc::new(|_| panic!("injected backend factory panic"));
        let result = TIER_DRIVER_TEST_FACTORY
            .scope(
                panic_factory,
                TierConfigMgr::prevalidate_candidate_owned(
                    empty_mgr(),
                    None,
                    TierCandidateMutation::Add(Box::new(build_rustfs_tier("COLD-A")), true),
                ),
            )
            .await;
        let err = match result {
            Ok(_) => panic!("a detached backend validation panic must be observable"),
            Err(err) => err,
        };

        let TierConfigUpdateError::Mutation(err) = err else {
            panic!("backend validation panic should retain the mutation error category");
        };
        assert!(err.message.contains("backend validation task failed"), "{}", err.message);
    }

    #[tokio::test]
    async fn peer_prepare_wait_releases_exclusive_locks_and_stale_candidate_cannot_publish() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("peer-wait lock fixture should persist");
        let started = Arc::new(Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let peer_calls = Arc::new(Mutex::new(Vec::new()));
        let peer: Arc<dyn TierMutationPeer> = Arc::new(BlockingPrepareTierMutationPeer {
            started: started.clone(),
            release: release.clone(),
            calls: peer_calls.clone(),
        });
        let update_manager = manager.clone();
        let update_store = store.clone();
        let update = tokio::spawn(async move {
            TIER_DRIVER_TEST_FACTORY
                .scope(
                    healthy_driver_factory(),
                    TIER_MUTATION_TEST_PEERS.scope(
                        vec![peer],
                        TierConfigMgr::update_candidate_with_config_lock(
                            &update_manager,
                            update_store,
                            TierCandidateMutation::Remove("COLD-A".to_string(), true),
                        ),
                    ),
                )
                .await
        });
        started.notified().await;

        let admin_guard = tokio::time::timeout(Duration::from_millis(100), TierConfigMgr::admin_update_lock(&manager))
            .await
            .expect("peer Prepare wait should release the local admin mutex");
        let competing_lock = store
            .new_ns_lock(RUSTFS_META_BUCKET, &tier_config_lock_path())
            .await
            .expect("peer-wait competing config lock should be created");
        let competing_guard = competing_lock
            .get_write_lock(Duration::from_millis(100))
            .await
            .expect("peer Prepare wait should release the namespace lock");
        let (mut competing, competing_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("peer-wait competing config should load");
        competing.tiers.insert("COLD-B".to_string(), build_rustfs_tier("COLD-B"));
        competing
            .save_tiering_config_if_current(store.clone(), competing_etag.as_deref())
            .await
            .expect("peer-wait competing config should commit");
        drop(competing_guard);
        drop(admin_guard);
        release.add_permits(1);

        let err = update
            .await
            .expect("peer-wait update task should join")
            .expect_err("the stale peer-wait candidate must not publish");
        let TierConfigUpdateError::Save(err) = err else {
            panic!("stale peer-wait candidate should fail precommit revalidation");
        };
        assert!(err.to_string().contains("configuration changed"), "{err}");
        assert_eq!(lock_unpoisoned(&peer_calls).as_slice(), &["prepare".to_string(), "abort".to_string()]);
        let current = load_tier_config_for_update(store)
            .await
            .expect("winning peer-wait config should reload")
            .0;
        assert!(current.tiers.contains_key("COLD-A"));
        assert!(current.tiers.contains_key("COLD-B"));
    }

    #[tokio::test]
    async fn peer_commit_wait_releases_exclusive_locks_after_durable_commit() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("peer-commit lock fixture should persist");
        let started = Arc::new(Notify::new());
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let peer: Arc<dyn TierMutationPeer> = Arc::new(BlockingCommitTierMutationPeer {
            started: started.clone(),
            release: release.clone(),
        });
        let update_manager = manager.clone();
        let update_store = store.clone();
        let update = tokio::spawn(async move {
            TIER_DRIVER_TEST_FACTORY
                .scope(
                    healthy_driver_factory(),
                    TIER_MUTATION_TEST_PEERS.scope(
                        vec![peer],
                        TierConfigMgr::update_candidate_with_config_lock(
                            &update_manager,
                            update_store,
                            TierCandidateMutation::Remove("COLD-A".to_string(), true),
                        ),
                    ),
                )
                .await
        });
        started.notified().await;

        let admin_guard = tokio::time::timeout(Duration::from_millis(100), TierConfigMgr::admin_update_lock(&manager))
            .await
            .expect("peer Commit wait should release the local admin mutex");
        let competing_lock = store
            .new_ns_lock(RUSTFS_META_BUCKET, &tier_config_lock_path())
            .await
            .expect("peer-commit competing config lock should be created");
        let competing_guard = competing_lock
            .get_write_lock(Duration::from_millis(100))
            .await
            .expect("durable coordinator Commit should release the namespace lock before peer Commit");
        let committed = load_tier_config_for_update(store.clone())
            .await
            .expect("committed config should be readable during peer Commit")
            .0;
        assert!(!committed.tiers.contains_key("COLD-A"));
        drop(competing_guard);
        drop(admin_guard);
        release.add_permits(1);

        update
            .await
            .expect("peer-commit update task should join")
            .expect("peer-commit update should publish after the peer responds");
        assert!(!manager.read().await.tiers.contains_key("COLD-A"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn reference_proof_releases_exclusive_locks_and_stale_candidate_cannot_publish() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("reference-proof lock fixture should persist");
        let barrier = tier_reference_proof_test_barrier();
        let scoped_barrier = barrier.clone();
        let update_manager = manager.clone();
        let update_store = store.clone();
        let update = tokio::spawn(async move {
            TIER_REFERENCE_PROOF_TEST_BARRIER
                .scope(
                    scoped_barrier,
                    TIER_DRIVER_TEST_FACTORY.scope(
                        healthy_driver_factory(),
                        TIER_MUTATION_TEST_PEERS.scope(
                            Vec::new(),
                            TierConfigMgr::update_candidate_with_config_lock(
                                &update_manager,
                                update_store,
                                TierCandidateMutation::Remove("COLD-A".to_string(), true),
                            ),
                        ),
                    ),
                )
                .await
        });
        barrier.arrived.notified().await;

        let admin_guard = tokio::time::timeout(Duration::from_millis(100), TierConfigMgr::admin_update_lock(&manager))
            .await
            .expect("reference proof should release the local admin update mutex");
        let competing_lock = store
            .new_ns_lock(RUSTFS_META_BUCKET, &tier_config_lock_path())
            .await
            .expect("competing config lock should be created");
        let competing_guard = competing_lock
            .get_write_lock(Duration::from_millis(100))
            .await
            .expect("durable Prepared must allow the namespace lock to release during reference proof");
        drop(admin_guard);

        let (mut competing, competing_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("competing config should load during reference proof");
        competing.tiers.insert("COLD-B".to_string(), build_rustfs_tier("COLD-B"));
        competing
            .save_tiering_config_if_current(store.clone(), competing_etag.as_deref())
            .await
            .expect("competing config should commit while the stale proof is paused");
        drop(competing_guard);

        barrier.release.add_permits(1);
        let err = update
            .await
            .expect("reference-proof update task should join")
            .expect_err("the stale reference-proof candidate must not publish");
        let TierConfigUpdateError::Save(err) = err else {
            panic!("stale reference proof should fail precommit revalidation");
        };
        assert!(err.to_string().contains("configuration changed"), "{err}");
        let current = load_tier_config_for_update(store)
            .await
            .expect("winning reference-proof config should reload")
            .0;
        assert!(current.tiers.contains_key("COLD-A"));
        assert!(current.tiers.contains_key("COLD-B"));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn reference_proof_rejects_a_changed_prepared_fence_revision_before_publish() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("prepared-fence revision fixture should persist");
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }

        let barrier = tier_reference_proof_test_barrier();
        let scoped_barrier = barrier.clone();
        let update_manager = manager.clone();
        let update_store = store.clone();
        let update = tokio::spawn(async move {
            TIER_REFERENCE_PROOF_TEST_BARRIER
                .scope(
                    scoped_barrier,
                    TIER_MUTATION_TEST_PEERS.scope(
                        Vec::new(),
                        TierConfigMgr::update_candidate_with_config_lock(
                            &update_manager,
                            update_store,
                            TierCandidateMutation::Remove("COLD-A".to_string(), true),
                        ),
                    ),
                )
                .await
        });
        barrier.arrived.notified().await;

        let unrelated = prepared_remove_intent("COLD-B", uuid::Uuid::from_u128(0x2237));
        TierConfigMgr::apply_prepared_mutation_intent_block(&manager, &unrelated)
            .await
            .expect("an unrelated prepared fence should advance the runtime revision");
        barrier.release.add_permits(1);

        let err = update
            .await
            .expect("tier update task should join")
            .expect_err("a reference proof cannot authorize publication across a fence revision change");
        let TierConfigUpdateError::Publish(err) = err else {
            panic!("the stale prepared-fence allowance should fail publication: {err:?}");
        };
        assert!(err.message.contains("changed before replacement"), "{err}");
        assert!(manager.read().await.tiers.contains_key("COLD-A"));
        assert!(
            load_tier_config_for_update(store)
                .await
                .expect("rejected tier config should remain readable")
                .0
                .tiers
                .contains_key("COLD-A")
        );
        TierConfigMgr::clear_prepared_mutation_intent_block(&manager, unrelated.mutation_id)
            .await
            .expect("unrelated test fence should clear");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn caller_cancellation_after_durable_prepare_does_not_hide_the_mutation() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        let mut persisted = empty_mgr();
        persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        persisted
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("cancel-after-prepare fixture should persist");
        let barrier = tier_reference_proof_test_barrier();
        let scoped_barrier = barrier.clone();
        let update_manager = manager.clone();
        let update_store = store.clone();
        let caller = tokio::spawn(async move {
            TIER_REFERENCE_PROOF_TEST_BARRIER
                .scope(
                    scoped_barrier,
                    TIER_DRIVER_TEST_FACTORY.scope(
                        healthy_driver_factory(),
                        TIER_MUTATION_TEST_PEERS.scope(
                            Vec::new(),
                            TierConfigMgr::update_candidate_with_config_lock(
                                &update_manager,
                                update_store,
                                TierCandidateMutation::Remove("COLD-A".to_string(), true),
                            ),
                        ),
                    ),
                )
                .await
        });
        barrier.arrived.notified().await;
        assert_eq!(
            TierConfigMgr::load_coordinator_mutation_intents(store.clone())
                .await
                .expect("durable coordinator intent should remain readable")
                .len(),
            1,
            "reference proof must start only after the durable Prepared intent exists"
        );

        caller.abort();
        barrier.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let current = load_tier_config_for_update(store.clone())
                    .await
                    .expect("detached mutation config should remain readable")
                    .0;
                if !current.tiers.contains_key("COLD-A") {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the detached Prepared mutation should converge after caller cancellation");
        assert!(!manager.read().await.tiers.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn precommit_revalidation_rejects_changed_or_expired_intent_identity() {
        let store = Arc::new(CasConfigStore::default());
        let mut config = empty_mgr();
        config.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        config
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("precommit revalidation fixture should persist");
        let (candidate, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("precommit revalidation fixture should load");
        let candidate_digest = tier_config_candidate_digest(&candidate).expect("candidate digest should build");
        let mut intent = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(40));
        intent.old_config_etag = version.clone();
        intent.candidate_digest = candidate_digest;
        intent.expires_at_unix_nanos = tier_mutation_intent_expiry_unix_nanos();
        save_coordinator_tier_mutation_intent(store.clone(), Some(&intent))
            .await
            .expect("prepared coordinator intent should persist");

        TierConfigMgr::revalidate_prepared_candidate_before_commit(
            store.clone(),
            version.as_deref(),
            candidate_digest,
            Some(&intent),
        )
        .await
        .expect("the exact live Prepared identity should revalidate");

        let stale_version = "stale-config-etag".to_string();
        let err = TierConfigMgr::revalidate_prepared_candidate_before_commit(
            store.clone(),
            Some(&stale_version),
            candidate_digest,
            Some(&intent),
        )
        .await
        .expect_err("an ETag mismatch must block config CAS");
        assert!(err.to_string().contains("configuration changed"), "{err}");

        let err = TierConfigMgr::revalidate_prepared_candidate_before_commit(
            store.clone(),
            version.as_deref(),
            [0xEE; 32],
            Some(&intent),
        )
        .await
        .expect_err("a candidate digest mismatch must block config CAS");
        assert!(err.to_string().contains("candidate digest changed"), "{err}");

        let mut mismatched_identity = intent.clone();
        mismatched_identity.affected_targets[0].old_backend_identity = Some([0xAB; 32]);
        let err = TierConfigMgr::revalidate_prepared_candidate_before_commit(
            store.clone(),
            version.as_deref(),
            candidate_digest,
            Some(&mismatched_identity),
        )
        .await
        .expect_err("a changed Prepared target identity must block config CAS");
        assert!(err.to_string().contains("intent changed"), "{err}");

        advance_tier_coordinator_mutation_intent_record_idempotent(
            store.clone(),
            intent.mutation_id,
            TierMutationIntentState::Aborted,
            None,
        )
        .await
        .expect("fixture intent should advance to Aborted");
        let err = TierConfigMgr::revalidate_prepared_candidate_before_commit(
            store.clone(),
            version.as_deref(),
            candidate_digest,
            Some(&intent),
        )
        .await
        .expect_err("an Aborted durable identity must block config CAS");
        assert!(err.to_string().contains("intent changed"), "{err}");

        let mut expired = prepared_remove_intent("COLD-A", uuid::Uuid::from_u128(41));
        expired.old_config_etag = version.clone();
        expired.candidate_digest = candidate_digest;
        expired.expires_at_unix_nanos = 1;
        save_coordinator_tier_mutation_intent(store.clone(), Some(&expired))
            .await
            .expect("expired Prepared fixture should persist");
        let err = TierConfigMgr::revalidate_prepared_candidate_before_commit(
            store,
            version.as_deref(),
            candidate_digest,
            Some(&expired),
        )
        .await
        .expect_err("an expired Prepared identity must block config CAS");
        assert!(err.to_string().contains("expired before config commit"), "{err}");
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn tier_config_update_waits_for_config_namespace_lock_before_loading() {
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        let config_file = tier_config_lock_path();
        let outer_lock = store
            .new_ns_lock(RUSTFS_META_BUCKET, &config_file)
            .await
            .expect("outer tier config lock should be created");
        let _outer_guard = outer_lock
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("outer tier config lock should be acquired");

        let result = temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
            TierConfigMgr::update_candidate_with_config_lock(&manager, store.clone(), TierCandidateMutation::Clear(true)).await
        })
        .await;
        let TierConfigUpdateError::Load(err) = result.expect_err("config mutation must wait behind the config namespace lock")
        else {
            panic!("config lock contention should fail before candidate load or mutation");
        };
        let rendered = err.to_string();
        assert!(rendered.to_ascii_lowercase().contains("timeout"), "{rendered}");
        assert!(rendered.contains(&config_file), "{rendered}");
        assert_eq!(
            store
                .lock_requests
                .lock()
                .expect("tier config lock request log should not poison")
                .as_slice(),
            &[
                (RUSTFS_META_BUCKET.to_string(), config_file.clone()),
                (RUSTFS_META_BUCKET.to_string(), config_file),
            ]
        );
    }

    #[tokio::test]
    async fn panicked_owned_update_restores_generation_and_reports_error() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let store = Arc::new(CasConfigStore::default());
        let mut candidate = empty_mgr();
        candidate.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        candidate
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("panic fixture should persist");
        let (_, version) = load_tier_config_for_update(store.clone())
            .await
            .expect("panic fixture should reload with an ETag");
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::panicking_in_use("panic")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;

        let err = TierConfigMgr::update_candidate_owned(
            &manager,
            store,
            candidate,
            version,
            TierCandidateMutation::Remove("COLD-A".to_string(), false),
            update,
            None,
        )
        .await
        .expect_err("mutation panic must be observable to the admin caller");
        let TierConfigUpdateError::Publish(err) = err else {
            panic!("mutation panic should be reported as a publish failure");
        };
        assert!(err.message.contains("panicked"));
        assert!(manager.read().await.tiers.contains_key("COLD-A"));
        let restored = TierConfigMgr::acquire_operation_lease(&manager, "COLD-A")
            .await
            .expect("the old generation must be restored after panic");
        assert!(restored.is_current(&manager).await);
        drop(restored);
        let guard = manager.read().await;
        let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
        assert!(!lock_unpoisoned(&runtime).draining.contains_key("COLD-A"));
    }

    #[tokio::test]
    async fn stale_tier_config_etag_allows_only_one_candidate_to_publish() {
        let store = Arc::new(CasConfigStore::default());
        empty_mgr()
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("initial conditional create should succeed");

        let (mut candidate_a, version_a) = load_tier_config_for_update(store.clone())
            .await
            .expect("first node should load config revision");
        let (mut candidate_b, version_b) = load_tier_config_for_update(store.clone())
            .await
            .expect("second node should load the same config revision");
        assert_eq!(version_a, version_b);
        candidate_a.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        candidate_b.tiers.insert("COLD-B".to_string(), build_rustfs_tier("COLD-B"));

        let save_a = candidate_a.save_tiering_config_if_current(store.clone(), version_a.as_deref());
        let save_b = candidate_b.save_tiering_config_if_current(store, version_b.as_deref());
        let (result_a, result_b) = tokio::join!(save_a, save_b);
        assert_ne!(result_a.is_ok(), result_b.is_ok(), "exactly one stale-ETag writer must win");

        let manager_a = TierConfigMgr::new();
        let manager_b = TierConfigMgr::new();
        if result_a.is_ok() {
            TierConfigMgr::publish_candidate(&manager_a, candidate_a, None)
                .await
                .expect("winning node should publish");
        }
        if result_b.is_ok() {
            TierConfigMgr::publish_candidate(&manager_b, candidate_b, None)
                .await
                .expect("winning node should publish");
        }
        assert_ne!(manager_a.read().await.empty(), manager_b.read().await.empty());
    }

    async fn assert_coordinator_commit_refresh_succeeds(mutation: TierCandidateMutation) {
        let adding = matches!(mutation, TierCandidateMutation::Add(..));
        let manager = TierConfigMgr::new();
        let store = Arc::new(CasConfigStore::default());
        if !adding {
            let mut persisted = empty_mgr();
            persisted.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
            persisted
                .save_tiering_config_if_current(store.clone(), None)
                .await
                .expect("existing tier fixture should persist");
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let barrier = Arc::new(CasCoordinatorCommitBarrier::default());
        *store.coordinator_commit_barrier.lock().await = Some(barrier.clone());
        let update_manager = manager.clone();
        let update_store = store.clone();
        let update = tokio::spawn(async move {
            TIER_DRIVER_TEST_FACTORY
                .scope(
                    healthy_driver_factory(),
                    TIER_MUTATION_TEST_PEERS.scope(
                        Vec::new(),
                        TierConfigMgr::update_candidate_with_config_lock(&update_manager, update_store, mutation),
                    ),
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), barrier.arrived.notified())
            .await
            .expect("mutation should reach coordinator commit after saving config");
        assert_eq!(
            load_tier_config_for_update(store.clone())
                .await
                .expect("saved config should be readable before coordinator commit")
                .0
                .tiers
                .contains_key("COLD-A"),
            adding
        );
        assert_eq!(
            TierConfigMgr::load_coordinator_mutation_intents(store.clone())
                .await
                .expect("coordinator intent should remain readable")[0]
                .state,
            TierMutationIntentState::Prepared
        );

        let lock_requests = lock_unpoisoned(&store.lock_requests).len();
        // Also exercise an independently scheduled refresh while the durable
        // coordinator record is still Prepared, before its commit notification.
        TierConfigMgr::request_committed_mutation_refresh(&manager).await;
        TIER_MUTATION_TEST_PEERS
            .scope(Vec::new(), async {
                let worker = TierConfigMgr::refresh_tier_config_handle_with(manager.clone(), store.clone());
                tokio::pin!(worker);
                tokio::time::timeout(Duration::from_secs(5), async {
                    while lock_unpoisoned(&store.lock_requests).len() == lock_requests {
                        tokio::select! {
                            _ = &mut worker => panic!("refresh worker must remain available"),
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("refresh should reconcile the Prepared record before waiting for the config lock");
                barrier.release.notify_one();
                let result = tokio::time::timeout(Duration::from_secs(5), async {
                    tokio::select! {
                        _ = &mut worker => panic!("refresh worker must remain available"),
                        result = update => result.expect("tier mutation task should join"),
                    }
                })
                .await
                .expect("tier mutation should finish with refresh running");
                result.expect("saved tier mutation must publish successfully on the first attempt");
            })
            .await;
        assert_eq!(manager.read().await.tiers.contains_key("COLD-A"), adding);
    }

    #[tokio::test]
    async fn tier_add_succeeds_with_refresh_during_coordinator_commit() {
        assert_coordinator_commit_refresh_succeeds(TierCandidateMutation::Add(Box::new(build_rustfs_tier("COLD-A")), true)).await;
    }

    #[tokio::test]
    async fn tier_remove_succeeds_with_refresh_during_coordinator_commit() {
        assert_coordinator_commit_refresh_succeeds(TierCandidateMutation::Remove("COLD-A".to_string(), true)).await;
    }

    async fn committed_refresh_fixture(fail_cleanup: bool) -> (Arc<RwLock<TierConfigMgr>>, Arc<CasConfigStore>, uuid::Uuid) {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }

        let store = Arc::new(CasConfigStore::default());
        empty_mgr()
            .save_tiering_config_if_current(store.clone(), None)
            .await
            .expect("committed refresh config fixture should persist");
        let (_, config_etag) = load_tier_config_for_update(store.clone())
            .await
            .expect("committed refresh config fixture should reload");
        let config_etag = config_etag.expect("committed refresh config fixture should have an ETag");
        let mutation_id = uuid::Uuid::from_u128(if fail_cleanup { 0xa1 } else { 0xa0 });
        let intent = committed_remove_intent("COLD-A", mutation_id, &config_etag);
        crate::services::tier::tier_mutation_intent::save_tier_mutation_intent_record(store.clone(), &intent)
            .await
            .expect("committed refresh intent should persist");
        TierConfigMgr::apply_committed_mutation_intent_block(&manager, &intent)
            .await
            .expect("committed refresh fence should install");
        if fail_cleanup {
            store.fail_next_delete_with_prefix(TIER_MUTATION_INTENT_RECORD_PREFIX).await;
        }
        (manager, store, mutation_id)
    }

    #[tokio::test]
    async fn committed_mutation_refresh_notification_is_rearmed_for_idempotent_commit() {
        let manager = TierConfigMgr::new();
        let intent = committed_remove_intent("COLD-A", uuid::Uuid::from_u128(0xa2), "etag-new");
        let notifier = TierConfigMgr::mutation_refresh_notifier(&manager).await;

        TierConfigMgr::apply_committed_mutation_intent_block(&manager, &intent)
            .await
            .expect("first committed fence should install");
        tokio::time::timeout(Duration::from_secs(1), notifier.notified())
            .await
            .expect("first committed fence should notify the refresh worker");

        TierConfigMgr::apply_committed_mutation_intent_block(&manager, &intent)
            .await
            .expect("idempotent committed fence should succeed");
        tokio::time::timeout(Duration::from_secs(1), notifier.notified())
            .await
            .expect("idempotent committed fence should notify the refresh worker again");
    }

    #[tokio::test]
    async fn committed_mutation_notification_refreshes_without_waiting_for_periodic_timer() {
        let (manager, store, mutation_id) = committed_refresh_fixture(false).await;
        TIER_MUTATION_TEST_PEERS
            .scope(Vec::new(), async {
                let worker = TierConfigMgr::refresh_tier_config_handle_with(manager.clone(), store.clone());
                tokio::pin!(worker);
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let converged = {
                            let guard = manager.read().await;
                            let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
                            lock_unpoisoned(&runtime).committed_mutation_blocks.is_empty()
                        };
                        if converged {
                            break;
                        }
                        tokio::select! {
                            _ = &mut worker => panic!("tier refresh worker must remain available for later notifications"),
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("committed notification should converge before the periodic timer");
            })
            .await;

        assert!(
            TierConfigMgr::load_tier_mutation_intents(store)
                .await
                .expect("committed refresh intent scan should succeed")
                .iter()
                .all(|intent| intent.mutation_id != mutation_id),
            "successful notification refresh should clean the committed intent"
        );
    }

    #[tokio::test]
    async fn committed_mutation_refresh_clears_fence_and_retries_after_cleanup_failure() {
        let (manager, store, mutation_id) = committed_refresh_fixture(true).await;
        TIER_MUTATION_TEST_PEERS
            .scope(Vec::new(), async {
                let worker = TierConfigMgr::refresh_tier_config_handle_with(manager.clone(), store.clone());
                tokio::pin!(worker);

                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        if store
                            .delete_log()
                            .await
                            .iter()
                            .any(|object| object.starts_with(TIER_MUTATION_INTENT_RECORD_PREFIX))
                        {
                            break;
                        }
                        tokio::select! {
                            _ = &mut worker => panic!("tier refresh worker must remain available after a failed cleanup"),
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("the first notification refresh should reach the injected cleanup failure");

                {
                    let guard = manager.read().await;
                    let runtime = registered_tier_driver_runtime(&guard).expect("runtime should remain registered");
                    assert!(
                        lock_unpoisoned(&runtime).committed_mutation_blocks.is_empty(),
                        "a published configuration must not restore its committed runtime fence after cleanup failure"
                    );
                }

                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        let converged = TierConfigMgr::load_tier_mutation_intents(store.clone())
                            .await
                            .expect("committed cleanup retry scan should succeed")
                            .iter()
                            .all(|intent| intent.mutation_id != mutation_id);
                        if converged {
                            break;
                        }
                        tokio::select! {
                            _ = &mut worker => panic!("tier refresh worker must remain available after retry"),
                            _ = tokio::task::yield_now() => {}
                        }
                    }
                })
                .await
                .expect("committed refresh should retry cleanup and converge");
            })
            .await;

        assert!(
            TierConfigMgr::load_tier_mutation_intents(store)
                .await
                .expect("retried committed refresh intent scan should succeed")
                .iter()
                .all(|intent| intent.mutation_id != mutation_id),
            "successful retry should clean the committed intent"
        );
    }

    #[test]
    fn committed_mutation_refresh_retry_delay_is_capped() {
        assert_eq!(tier_mutation_refresh_retry_delay(0), Duration::from_secs(1));
        assert_eq!(tier_mutation_refresh_retry_delay(1), Duration::from_secs(2));
        assert_eq!(tier_mutation_refresh_retry_delay(4), Duration::from_secs(16));
        assert_eq!(tier_mutation_refresh_retry_delay(5), TIER_MUTATION_REFRESH_RETRY_CAP);
        assert_eq!(tier_mutation_refresh_retry_delay(u32::MAX), TIER_MUTATION_REFRESH_RETRY_CAP);
    }

    #[test]
    fn terminal_mutation_cleanup_read_failure_keeps_retry_armed() {
        assert!(TierConfigMgr::terminal_mutation_cleanup_requires_retry(Ok(true)));
        assert!(!TierConfigMgr::terminal_mutation_cleanup_requires_retry(Ok(false)));
        assert!(TierConfigMgr::terminal_mutation_cleanup_requires_retry(Err(io::Error::other(
            "injected durable cleanup read failure",
        ))));
    }

    #[test]
    fn legacy_refresh_method_signature_remains_callable() {
        async fn call_legacy_refresh(manager: &mut TierConfigMgr, api: Arc<ECStore>) {
            manager.refresh_tier_config(api).await;
        }

        let _ = call_legacy_refresh;
    }

    #[tokio::test(start_paused = true)]
    async fn periodic_refresh_timer_does_not_tick_on_startup() {
        let period = Duration::from_secs(60);
        let mut timer = delayed_tier_refresh_interval(period);
        assert!(
            tokio::time::timeout(Duration::ZERO, timer.tick()).await.is_err(),
            "periodic reload must not duplicate the startup reload"
        );
        tokio::time::advance(period).await;
        tokio::time::timeout(Duration::ZERO, timer.tick())
            .await
            .expect("the first periodic reload should become ready after one interval");
    }
}
