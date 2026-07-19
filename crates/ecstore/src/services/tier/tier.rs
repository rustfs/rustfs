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
#![allow(unused_must_use)]
#![allow(clippy::all)]

use byteorder::{ByteOrder, LittleEndian};
use bytes::Bytes;
use futures::FutureExt;
use http::HeaderMap;
use http::status::StatusCode;
use lazy_static::lazy_static;
use rand::{Rng, RngExt};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
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
    sync::RwLock,
    time::{Instant, interval, interval_at, timeout_at},
};
use tracing::{debug, error, info, warn};

use crate::client::admin_handler_utils::AdminError;
use crate::error::{Error, Result, StorageError};
use crate::services::tier::{
    tier_admin::TierCreds,
    tier_config::{TierConfig, TierType},
    tier_handlers::{ERR_TIER_ALREADY_EXISTS, ERR_TIER_NAME_NOT_UPPERCASE, ERR_TIER_NOT_FOUND, ERR_TIER_RESERVED_NAME},
    warm_backend::{WarmBackend, check_warm_backend, new_warm_backend},
};
use crate::storage_api_contracts::{
    object::{
        DeletedObject, EcstoreObjectIO, EcstoreObjectOperations, HTTPPreconditions, ObjectIO, ObjectOperations, ObjectToDelete,
    },
    range::HTTPRangeSpec,
};
use crate::{
    config::com::{CONFIG_PREFIX, read_config, read_config_with_metadata},
    disk::{MIGRATING_META_BUCKET, RUSTFS_META_BUCKET},
    object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader},
    runtime::sources as runtime_sources,
    store::ECStore,
};
use rustfs_filemeta::FileInfo;
use rustfs_rio::HashReader;
use rustfs_utils::path::{SLASH_SEPARATOR, path_join};
use s3s::S3ErrorCode;

use super::{
    tier_handlers::{ERR_TIER_BUCKET_NOT_FOUND, ERR_TIER_CONNECT_ERR, ERR_TIER_INVALID_CREDENTIALS, ERR_TIER_PERM_ERR},
    warm_backend::WarmBackendImpl,
};

const TIER_CFG_REFRESH: Duration = Duration::from_secs(15 * 60);
const TIER_OPERATION_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const TIER_REMOTE_VALIDATION_TIMEOUT: Duration = Duration::from_secs(30);

fn delayed_tier_refresh_interval(period: Duration) -> tokio::time::Interval {
    interval_at(Instant::now() + period, period)
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

async fn build_warm_backend(tier: &TierConfig, probe: bool) -> std::result::Result<WarmBackendImpl, AdminError> {
    #[cfg(test)]
    let test_barrier = { lock_unpoisoned(&TIER_DRIVER_BUILD_BARRIER).clone() };
    #[cfg(test)]
    if let Some(barrier) = test_barrier
        && barrier.tier_name == tier.name
    {
        barrier.arrived.notify_one();
        barrier
            .release
            .acquire()
            .await
            .expect("tier driver build test barrier should stay open")
            .forget();
    }
    new_warm_backend(tier, probe).await
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
    next_generation: u64,
    next_drain_epoch: u64,
    admin_updates: Arc<tokio::sync::Mutex<()>>,
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
    published: bool,
}

struct PreparedTierDriver {
    tier_name: String,
    config_fingerprint: TierDriverFingerprint,
    backend_identity: TierDestinationId,
    driver: SharedWarmBackend,
}

impl TierPublishTransition {
    async fn wait_for_active_leases(&self) -> std::result::Result<(), AdminError> {
        let deadline = Instant::now() + TIER_OPERATION_DRAIN_TIMEOUT;
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
    Add(TierConfig, bool),
    Edit(String, TierCreds),
    Remove(String, bool),
    Clear(bool),
}

impl TierCandidateMutation {
    fn target_tiers(&self, manager: &TierConfigMgr, candidate: &TierConfigMgr) -> HashSet<String> {
        let mut targets = changed_tier_names(manager, candidate);
        match self {
            Self::Add(config, _) => {
                targets.insert(config.name.clone());
            }
            Self::Edit(tier_name, _) | Self::Remove(tier_name, _) => {
                targets.insert(tier_name.clone());
            }
            Self::Clear(_) => {
                targets.extend(manager.tiers.keys().chain(candidate.tiers.keys()).cloned());
            }
        }
        targets
    }

    async fn apply(self, candidate: &mut TierConfigMgr) -> std::result::Result<Option<String>, AdminError> {
        match self {
            Self::Add(config, force) => {
                let tier_name = config.name.clone();
                candidate.add(config, force).await?;
                Ok(Some(tier_name))
            }
            Self::Edit(tier_name, credentials) => {
                candidate.edit(&tier_name, credentials).await?;
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
        }
    }
}

async fn apply_tier_candidate_mutation(
    mutation: TierCandidateMutation,
    candidate: &mut TierConfigMgr,
    deadline: Instant,
) -> std::result::Result<Option<String>, AdminError> {
    match timeout_at(deadline, mutation.apply(candidate)).await {
        Ok(result) => result,
        Err(_) => {
            let mut err = ERR_TIER_BACKEND_IN_USE.clone();
            err.message = "Timed out validating the remote tier mutation".to_string();
            Err(err)
        }
    }
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

fn tier_backend_identity(config: &TierConfig) -> io::Result<TierDestinationId> {
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
        TierType::S3 => prefix.trim_matches('/'),
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

struct TierDriverGeneration {
    tier_name: Arc<str>,
    generation: DriverRevision,
    // Process-local only: this may reflect credential changes and must never be persisted or logged.
    config_fingerprint: TierDriverFingerprint,
    // Durable cleanup identity: routing fields only, with all credentials excluded.
    backend_identity: TierDestinationId,
    driver: SharedWarmBackend,
    accepting: AtomicBool,
    active_leases: AtomicUsize,
    drained: tokio::sync::Notify,
}

struct SharedWarmBackendProxy(SharedWarmBackend);

#[async_trait::async_trait]
impl WarmBackend for SharedWarmBackendProxy {
    async fn put(&self, object: &str, r: crate::client::transition_api::ReaderImpl, length: i64) -> io::Result<String> {
        self.0.put(object, r, length).await
    }

    async fn put_with_meta(
        &self,
        object: &str,
        r: crate::client::transition_api::ReaderImpl,
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
    ) -> io::Result<crate::client::transition_api::ReadCloser> {
        self.0.get(object, rv, opts).await
    }

    async fn remove(&self, object: &str, rv: &str) -> io::Result<()> {
        self.0.remove(object, rv).await
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
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| active.checked_add(1))
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
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |active| active.checked_sub(1));
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
    tiers: HashMap<String, ExternalTierConfig>,
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

fn tier_config_path(file: &str) -> String {
    format!("{}{}{}", CONFIG_PREFIX, SLASH_SEPARATOR, file)
}

fn tier_hint_for_type(tier_type: TierType) -> Option<&'static str> {
    match tier_type {
        TierType::RustFS => Some("rustfs"),
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

    let hinted = tier_type_from_hint(ext.tier_type_hint.as_deref());
    let tier_type = if let Some(h) = hinted {
        h
    } else {
        match ext.tier_type {
            EXTERNAL_TIER_TYPE_S3 => TierType::S3,
            EXTERNAL_TIER_TYPE_AZURE => TierType::Azure,
            EXTERNAL_TIER_TYPE_GCS => TierType::GCS,
            EXTERNAL_TIER_TYPE_MINIO => TierType::MinIO,
            _ => TierType::Unsupported,
        }
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
                creds: gcs.creds.clone(),
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
    let mut tiers = HashMap::with_capacity(cfg.tiers.len());
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
        let cfg: TierConfigMgr = serde_json::from_slice(data)?;
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

        let d = new_warm_backend(&tier_config, true).await?;

        if !force {
            let in_use = d.in_use().await;
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
        let d = self.get_driver(tier_name).await;
        if let Err(err) = d {
            if err.code == ERR_TIER_NOT_FOUND.code {
                return Ok(());
            } else {
                return Err(err);
            }
        }
        if !force {
            if let Ok(driver) = d {
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
        }
        self.tiers.remove(tier_name);
        self.revoke_driver(tier_name);
        Ok(())
    }

    pub async fn verify(&mut self, tier_name: &str) -> std::result::Result<(), std::io::Error> {
        let d = match self.get_driver(tier_name).await {
            Ok(d) => d,
            Err(err) => {
                return Err(std::io::Error::other(err));
            }
        };
        if let Err(err) = check_warm_backend(Some(d)).await {
            return Err(std::io::Error::other(err));
        } else {
            return Ok(());
        }
    }

    pub fn empty(&self) -> bool {
        self.list_tiers().len() == 0
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
        for (_, tier) in self.tiers.iter() {
            let tier = tier.clone();
            tier_cfgs.push(tier);
        }
        tier_cfgs
    }

    pub fn get(&self, tier_name: &str) -> Option<TierConfig> {
        for (tier_name2, tier) in self.tiers.iter() {
            if tier_name == tier_name2 {
                return Some(tier.clone());
            }
        }
        None
    }

    pub async fn edit(&mut self, tier_name: &str, creds: TierCreds) -> std::result::Result<(), AdminError> {
        self.ensure_generation_is_idle(tier_name)?;
        let (tier_type, exists) = self.is_tier_name_in_use(tier_name);
        if !exists {
            return Err(ERR_TIER_NOT_FOUND.clone());
        }

        let mut tier_config = self.tiers[tier_name].clone();
        match tier_type {
            TierType::S3 => {
                if let Some(s3) = tier_config.s3.as_mut() {
                    if creds.aws_role {
                        s3.aws_role = true
                    }
                    if creds.aws_role_web_identity_token_file != "" && creds.aws_role_arn != "" {
                        s3.aws_role_arn = creds.aws_role_arn;
                        s3.aws_role_web_identity_token_file = creds.aws_role_web_identity_token_file;
                    }
                    if creds.access_key != "" && creds.secret_key != "" {
                        s3.access_key = creds.access_key;
                        s3.secret_key = creds.secret_key;
                    }
                }
            }
            TierType::RustFS => {
                if let Some(rustfs) = tier_config.rustfs.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    rustfs.access_key = creds.access_key;
                    rustfs.secret_key = creds.secret_key;
                }
            }
            TierType::MinIO => {
                if let Some(compatible_backend) = tier_config.minio.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    compatible_backend.access_key = creds.access_key;
                    compatible_backend.secret_key = creds.secret_key;
                }
            }
            TierType::Aliyun => {
                if let Some(aliyun) = tier_config.aliyun.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    aliyun.access_key = creds.access_key;
                    aliyun.secret_key = creds.secret_key;
                }
            }
            TierType::Tencent => {
                if let Some(tencent) = tier_config.tencent.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    tencent.access_key = creds.access_key;
                    tencent.secret_key = creds.secret_key;
                }
            }
            TierType::Huaweicloud => {
                if let Some(huaweicloud) = tier_config.huaweicloud.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    huaweicloud.access_key = creds.access_key;
                    huaweicloud.secret_key = creds.secret_key;
                }
            }
            TierType::Azure => {
                if let Some(azure) = tier_config.azure.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    azure.access_key = creds.access_key;
                    azure.secret_key = creds.secret_key;
                }
            }
            TierType::GCS => {
                if let Some(gcs) = tier_config.gcs.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    gcs.creds = creds.access_key; //creds.creds_json
                }
            }
            TierType::R2 => {
                if let Some(r2) = tier_config.r2.as_mut() {
                    if creds.access_key == "" || creds.secret_key == "" {
                        return Err(ERR_TIER_MISSING_CREDENTIALS.clone());
                    }
                    r2.access_key = creds.access_key;
                    r2.secret_key = creds.secret_key;
                }
            }
            _ => (),
        }

        let d = new_warm_backend(&tier_config, true).await?;
        self.revoke_driver(tier_name);
        self.tiers.insert(tier_name.to_string(), tier_config);
        self.replace_driver(tier_name, d)?;
        Ok(())
    }

    pub async fn get_driver<'a>(&'a mut self, tier_name: &str) -> std::result::Result<&'a WarmBackendImpl, AdminError> {
        if registered_tier_driver_runtime(self).is_some_and(|runtime| lock_unpoisoned(&runtime).draining.contains_key(tier_name))
        {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier configuration is being replaced".to_string();
            return Err(err);
        }
        // Return cached driver if present
        if self.driver_cache.contains_key(tier_name) {
            return Ok(self.driver_cache.get(tier_name).expect("Driver not found in cache"));
        }

        // Get tier configuration and create new driver
        let tier_config = self.tiers.get(tier_name).ok_or_else(|| ERR_TIER_NOT_FOUND.clone())?;

        let driver = new_warm_backend(tier_config, false).await?;

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
            if runtime_guard.draining.contains_key(tier_name) {
                let mut err = ERR_TIER_INVALID_CONFIG.clone();
                err.message = "Remote tier configuration is being replaced".to_string();
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
            if lock_unpoisoned(&runtime).draining.contains_key(tier_name) {
                let mut err = ERR_TIER_INVALID_CONFIG.clone();
                err.message = "Remote tier configuration is being replaced".to_string();
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
        if runtime_guard.draining.contains_key(tier_name) {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier configuration is being replaced".to_string();
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

    async fn admin_update_lock(handle: &Arc<RwLock<Self>>) -> tokio::sync::OwnedMutexGuard<()> {
        let update_lock = {
            let manager = handle.read().await;
            let runtime = tier_driver_runtime(handle, &manager);
            lock_unpoisoned(&runtime).admin_updates.clone()
        };
        update_lock.lock_owned().await
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

    fn begin_publish_transition(
        handle: &Arc<RwLock<Self>>,
        manager: &mut Self,
        candidate: &Self,
    ) -> std::result::Result<TierPublishTransition, AdminError> {
        let changed = changed_tier_names(manager, candidate);
        let replaced_destinations = replaced_tier_destinations(manager, candidate)?;
        Self::begin_tier_transition_with_destinations(handle, manager, changed, replaced_destinations)
    }

    fn begin_tier_transition(
        handle: &Arc<RwLock<Self>>,
        manager: &mut Self,
        changed: HashSet<String>,
    ) -> std::result::Result<TierPublishTransition, AdminError> {
        Self::begin_tier_transition_with_destinations(handle, manager, changed, HashMap::new())
    }

    fn begin_tier_transition_with_destinations(
        handle: &Arc<RwLock<Self>>,
        manager: &mut Self,
        changed: HashSet<String>,
        replaced_destinations: HashMap<String, TierConfig>,
    ) -> std::result::Result<TierPublishTransition, AdminError> {
        let runtime = tier_driver_runtime(handle, manager);
        for tier_name in replaced_destinations.keys() {
            if !lock_unpoisoned(&runtime).generations.contains_key(tier_name)
                && let Some(driver) = manager.driver_cache.remove(tier_name)
            {
                manager.replace_driver(tier_name, driver)?;
            }
        }
        Self::begin_tier_transition_in_runtime(runtime, changed, replaced_destinations)
    }

    fn begin_tier_transition_in_runtime(
        runtime: Arc<Mutex<TierDriverRuntime>>,
        changed: HashSet<String>,
        replaced_destinations: HashMap<String, TierConfig>,
    ) -> std::result::Result<TierPublishTransition, AdminError> {
        let count = u64::try_from(changed.len()).map_err(|_| {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier replacement count exceeds the supported limit".to_string();
            err
        })?;
        let mut runtime_guard = lock_unpoisoned(&runtime);
        if changed.iter().any(|tier_name| runtime_guard.draining.contains_key(tier_name)) {
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
            published: false,
        })
    }

    async fn publish_candidate_inner(
        handle: &Arc<RwLock<Self>>,
        candidate: Self,
        driver_tier: Option<&str>,
    ) -> std::result::Result<(), AdminError> {
        let transition = {
            let mut manager = handle.write().await;
            Self::begin_publish_transition(handle, &mut manager, &candidate)?
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
                    Some(PreparedTierDriver {
                        tier_name: tier_name.to_string(),
                        config_fingerprint,
                        backend_identity,
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
        let prepared_generation = if let Some(prepared) = prepared_driver {
            let generation = runtime.next_generation.checked_add(1).ok_or_else(|| {
                let mut err = ERR_TIER_INVALID_CONFIG.clone();
                err.message = "Remote tier driver generation exhausted".to_string();
                err
            })?;
            let entry = Arc::new(TierDriverGeneration {
                tier_name: Arc::from(prepared.tier_name.as_str()),
                generation,
                config_fingerprint: prepared.config_fingerprint,
                backend_identity: prepared.backend_identity,
                driver: prepared.driver.clone(),
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

    async fn publish_candidate_owned(
        handle: &Arc<RwLock<Self>>,
        candidate: Self,
        driver_tier: Option<String>,
        update: tokio::sync::OwnedMutexGuard<()>,
    ) -> std::result::Result<(), AdminError> {
        let handle = handle.clone();
        tokio::spawn(async move {
            match AssertUnwindSafe(async move {
                let _update = update;
                Self::publish_candidate_inner(&handle, candidate, driver_tier.as_deref()).await
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
    ) -> std::result::Result<(), TierConfigUpdateError>
    where
        S: EcstoreObjectIO + 'static,
    {
        let handle = handle.clone();
        tokio::spawn(async move {
            match AssertUnwindSafe(async move {
                let _update = update;
                let transition = {
                    let mut manager = handle.write().await;
                    let target_tiers = mutation.target_tiers(&manager, &candidate);
                    Self::begin_tier_transition(&handle, &mut manager, target_tiers).map_err(TierConfigUpdateError::Publish)?
                };
                transition
                    .wait_for_active_leases()
                    .await
                    .map_err(TierConfigUpdateError::Publish)?;
                let driver_tier =
                    apply_tier_candidate_mutation(mutation, &mut candidate, Instant::now() + TIER_REMOTE_VALIDATION_TIMEOUT)
                        .await
                        .map_err(TierConfigUpdateError::Mutation)?;
                candidate
                    .save_tiering_config_if_current(api, version.as_deref())
                    .await
                    .map_err(TierConfigUpdateError::Save)?;
                Self::publish_candidate_after_drain(&handle, candidate, driver_tier.as_deref(), transition)
                    .await
                    .map_err(TierConfigUpdateError::Publish)
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
        })
        .await
        .map_err(|err| TierConfigUpdateError::Publish(Self::publish_task_error(err)))?
    }

    pub async fn reload_handle(handle: &Arc<RwLock<Self>>, api: Arc<ECStore>) -> io::Result<()> {
        let update = Self::admin_update_lock(handle).await;
        let candidate = load_tier_config(api).await?;
        Self::publish_candidate_owned(handle, candidate, None, update)
            .await
            .map_err(io::Error::other)
    }

    pub async fn add_and_save(
        handle: &Arc<RwLock<Self>>,
        api: Arc<ECStore>,
        tier_config: TierConfig,
        force: bool,
    ) -> std::result::Result<(), TierConfigUpdateError> {
        let update = Self::admin_update_lock(handle).await;
        let (candidate, version) = load_tier_config_for_update(api.clone())
            .await
            .map_err(TierConfigUpdateError::Load)?;
        Self::update_candidate_owned(handle, api, candidate, version, TierCandidateMutation::Add(tier_config, force), update)
            .await
    }

    pub async fn edit_and_save(
        handle: &Arc<RwLock<Self>>,
        api: Arc<ECStore>,
        tier_name: &str,
        credentials: TierCreds,
    ) -> std::result::Result<(), TierConfigUpdateError> {
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
        )
        .await
    }

    pub async fn remove_and_save(
        handle: &Arc<RwLock<Self>>,
        api: Arc<ECStore>,
        tier_name: &str,
        force: bool,
    ) -> std::result::Result<(), TierConfigUpdateError> {
        Self::remove_and_save_with(handle, api, tier_name, force).await
    }

    async fn remove_and_save_with<S>(
        handle: &Arc<RwLock<Self>>,
        api: Arc<S>,
        tier_name: &str,
        force: bool,
    ) -> std::result::Result<(), TierConfigUpdateError>
    where
        S: EcstoreObjectIO + 'static,
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
        )
        .await
    }

    pub async fn clear_and_save(
        handle: &Arc<RwLock<Self>>,
        api: Arc<ECStore>,
        force: bool,
    ) -> std::result::Result<(), TierConfigUpdateError> {
        Self::clear_and_save_with(handle, api, force).await
    }

    async fn clear_and_save_with<S>(
        handle: &Arc<RwLock<Self>>,
        api: Arc<S>,
        force: bool,
    ) -> std::result::Result<(), TierConfigUpdateError>
    where
        S: EcstoreObjectIO + 'static,
    {
        let update = Self::admin_update_lock(handle).await;
        let (candidate, version) = load_tier_config_for_update(api.clone())
            .await
            .map_err(TierConfigUpdateError::Load)?;
        Self::update_candidate_owned(handle, api, candidate, version, TierCandidateMutation::Clear(force), update).await
    }

    pub async fn verify_without_manager_lock(handle: &Arc<RwLock<Self>>, tier_name: &str) -> std::result::Result<(), io::Error> {
        let lease = Self::acquire_operation_lease(handle, tier_name)
            .await
            .map_err(io::Error::other)?;
        let driver: WarmBackendImpl = Box::new(SharedWarmBackendProxy(lease.inner.driver.clone()));
        check_warm_backend(Some(&driver)).await.map_err(io::Error::other)
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
        let config_fingerprint = self
            .tiers
            .get(tier_name)
            .ok_or_else(|| ERR_TIER_NOT_FOUND.clone())
            .and_then(|config| {
                tier_config_fingerprint(tier_name, config).map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })
            })?;
        let backend_identity = self
            .tiers
            .get(tier_name)
            .ok_or_else(|| ERR_TIER_NOT_FOUND.clone())
            .and_then(|config| {
                tier_backend_identity(config).map_err(|err| {
                    let mut admin_err = ERR_TIER_INVALID_CONFIG.clone();
                    admin_err.message = err.to_string();
                    admin_err
                })
            })?;
        let mut runtime_guard = lock_unpoisoned(&runtime);
        let generation = runtime_guard.next_generation.checked_add(1).ok_or_else(|| {
            let mut err = ERR_TIER_INVALID_CONFIG.clone();
            err.message = "Remote tier driver generation exhausted".to_string();
            err
        })?;
        let driver: SharedWarmBackend = Arc::from(driver);
        let entry = Arc::new(TierDriverGeneration {
            tier_name: Arc::from(tier_name),
            generation,
            config_fingerprint,
            backend_identity,
            driver: driver.clone(),
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

    #[cfg(feature = "test-util")]
    pub(crate) fn install_test_driver(
        &mut self,
        tier_name: &str,
        driver: WarmBackendImpl,
    ) -> std::result::Result<(), AdminError> {
        self.replace_driver(tier_name, driver)
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
        let blocked = runtime.draining.contains_key(tier_name)
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

    fn retire_driver(&mut self, tier_name: &str) {
        self.revoke_driver(tier_name);
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
            let mut transition =
                Self::begin_tier_transition_in_runtime(runtime, changed, replaced_destinations).map_err(io::Error::other)?;
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
        if registered_tier_driver_runtime(self).is_some_and(|runtime| !lock_unpoisoned(&runtime).draining.is_empty()) {
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
                    if !runtime.draining.is_empty()
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
        if registered_tier_driver_runtime(self).is_some_and(|runtime| !lock_unpoisoned(&runtime).draining.is_empty()) {
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

        self.save_config_with_opts(
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
        debug!("save tier config:{}", file);
        let mut put_data = PutObjReader::from_vec(data.to_vec());
        let _ = api.put_object(RUSTFS_META_BUCKET, file, &mut put_data, opts).await?;
        Ok(())
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
        //let r = rand.New(rand.NewSource(time.Now().UnixNano()));
        let r = rand::rng().random_range(0.0..1.0);
        let rand_interval = || Duration::from_secs((r * 60_f64).round() as u64);

        let refresh_interval = TIER_CFG_REFRESH + rand_interval();
        let mut t = delayed_tier_refresh_interval(refresh_interval);
        loop {
            select! {
                _ = t.tick() => {
                    if let Err(err) = Self::reload_handle(&handle, api.clone()).await {
                      info!("{}", err);
                    }
                }
                else => ()
            }
            t.reset();
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
async fn load_tier_config(api: Arc<ECStore>) -> std::result::Result<TierConfigMgr, std::io::Error> {
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

async fn load_tier_config_for_update<S>(api: Arc<S>) -> std::result::Result<(TierConfigMgr, Option<String>), std::io::Error>
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
    let config_file = tier_config_path(TIER_CONFIG_FILE);
    match read_config_with_metadata(api.clone(), &config_file, &ObjectOptions::default()).await {
        Ok((data, object_info)) => {
            let etag = object_info
                .etag
                .filter(|etag| !etag.trim().is_empty())
                .ok_or_else(|| io::Error::other("tier configuration object is missing an ETag"))?;
            Ok((decode_tiering_config_blob(&data)?, Some(etag)))
        }
        Err(err) if is_err_config_not_found(&err) => {
            let legacy_file = tier_config_path(TIER_CONFIG_LEGACY_FILE);
            match read_config(api, &legacy_file).await {
                Ok(data) => Ok((TierConfigMgr::unmarshal(&data)?, None)),
                Err(legacy_err) if is_err_config_not_found(&legacy_err) => Ok((
                    TierConfigMgr {
                        driver_cache: HashMap::new(),
                        tiers: HashMap::new(),
                        last_refreshed_at: OffsetDateTime::now_utc(),
                    },
                    None,
                )),
                Err(legacy_err) => Err(io::Error::other(legacy_err)),
            }
        }
        Err(err) => Err(io::Error::other(err)),
    }
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
    //   * error paths that return *before* `new_warm_backend` constructs a
    //     client (name validation, duplicate detection, unsupported type,
    //     missing backend payload, missing credentials);
    //   * a `MockWarmBackend` injected directly into `driver_cache`, so
    //     `get_driver` returns it from cache without any network I/O, which
    //     lets us drive `remove`/`verify` through every branch.
    // ---------------------------------------------------------------------

    use crate::client::transition_api::{ReadCloser, ReaderImpl};
    use crate::services::tier::warm_backend::{WarmBackend, WarmBackendGetOpts};

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

    /// A fully offline `WarmBackend` used to exercise the driver-facing
    /// branches of `remove`/`verify` without touching a remote tier.
    struct MockWarmBackend {
        /// `Some(b)` -> `in_use()` returns `Ok(b)`; `None` -> returns `Err`.
        in_use_value: Option<bool>,
        /// When false, put/get/remove all fail (drives `verify` error paths).
        healthy: bool,
    }

    #[async_trait::async_trait]
    impl WarmBackend for MockWarmBackend {
        async fn put(&self, _object: &str, _r: ReaderImpl, _length: i64) -> std::result::Result<String, std::io::Error> {
            if self.healthy {
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
                Ok(())
            } else {
                Err(std::io::Error::other("mock remove failed"))
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

    // ---- add ------------------------------------------------------------

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
    async fn test_edit_rejects_missing_credentials_for_rustfs() {
        let mut mgr = empty_mgr();
        mgr.tiers.insert("COLD-R".to_string(), build_rustfs_tier("COLD-R"));

        // Empty access/secret keys => rejected before any driver rebuild.
        let err = mgr
            .edit("COLD-R", TierCreds::default())
            .await
            .expect_err("empty credentials must be rejected");
        assert_eq!(err.code, ERR_TIER_MISSING_CREDENTIALS.code);
    }

    #[tokio::test]
    async fn test_edit_rejects_missing_credentials_for_minio() {
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
            .edit("COLD-M", TierCreds::default())
            .await
            .expect_err("empty credentials must be rejected");
        assert_eq!(err.code, ERR_TIER_MISSING_CREDENTIALS.code);
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
            },
        );
        let mutation = TierCandidateMutation::Remove("COLD-A".to_string(), true);
        mutation
            .apply(&mut candidate)
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
            },
        );
        let err = TierCandidateMutation::Remove("COLD-A".to_string(), false)
            .apply(&mut candidate)
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
            },
        );

        mgr.verify("COLD-A")
            .await
            .expect_err("an unhealthy backend must fail verification");
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
        // secret_key is a serialized field (unlike Clone, marshal does not redact).
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
    fn test_external_blob_roundtrip_gcs_preserves_creds() {
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
                    creds: "service-account-json".to_string(),
                    bucket: "gbucket".to_string(),
                    prefix: "gp".to_string(),
                    region: "us".to_string(),
                    storage_class: "NEARLINE".to_string(),
                }),
                ..Default::default()
            },
        );

        let bytes = encode_external_tiering_config_blob(&mgr).expect("encode gcs tier");
        let decoded = decode_external_tiering_config_blob(&bytes).expect("decode gcs tier");
        let tier = decoded.tiers.get("COLD-G").expect("gcs tier survives roundtrip");
        assert_eq!(tier.tier_type.as_lowercase(), "gcs");
        assert_eq!(tier.gcs.as_ref().expect("gcs payload survives").creds, "service-account-json");
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
            Ok(BufReader::new(Cursor::new(Vec::new())))
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
            Ok(())
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

        tokio::time::timeout(Duration::from_millis(100), manager.read())
            .await
            .expect("cold driver construction must not block manager readers");
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
        tokio::time::timeout(Duration::from_millis(100), manager.read())
            .await
            .expect("slow verify must not hold the manager lock");
        release.add_permits(1);
        verify.await.expect("verify task should join").expect("verify should finish");
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
        tokio::time::timeout(Duration::from_secs(1), manager.read())
            .await
            .expect("manager reads must not wait for tier A leases");
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
        assert!(lock_unpoisoned(&runtime).generations.get("COLD-A").is_none());
    }

    #[derive(Debug, Default)]
    struct CasConfigStore {
        state: tokio::sync::Mutex<Option<(Vec<u8>, String)>>,
        next_etag: AtomicUsize,
        fail_put: AtomicBool,
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
            let state = self.state.lock().await;
            let (data, etag) = state.as_ref().ok_or(Error::ConfigNotFound)?;
            Ok(GetObjectReader {
                stream: Box::new(Cursor::new(data.clone())),
                object_info: ObjectInfo {
                    bucket: bucket.to_string(),
                    name: object.to_string(),
                    size: data.len() as i64,
                    actual_size: data.len() as i64,
                    etag: Some(etag.clone()),
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
            let mut payload = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut data.stream, &mut payload).await?;
            let mut state = self.state.lock().await;
            match state.as_ref() {
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
            let etag = format!("etag-{}", self.next_etag.fetch_add(1, Ordering::SeqCst) + 1);
            *state = Some((payload.clone(), etag.clone()));
            Ok(ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                size: payload.len() as i64,
                actual_size: payload.len() as i64,
                etag: Some(etag),
                ..Default::default()
            })
        }
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
    async fn failed_owned_update_restores_generation_and_reports_save_error() {
        let manager = TierConfigMgr::new();
        {
            let mut guard = manager.write().await;
            install_lease_backend(&mut guard, "COLD-A", LeaseTestBackend::ready("old"));
        }
        let store = Arc::new(CasConfigStore::default());
        store.fail_put.store(true, Ordering::SeqCst);
        let mut candidate = empty_mgr();
        candidate.tiers.insert("COLD-A".to_string(), build_rustfs_tier("COLD-A"));
        candidate
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::ready("candidate")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;

        let err = TierConfigMgr::update_candidate_owned(
            &manager,
            store.clone(),
            candidate,
            None,
            TierCandidateMutation::Remove("COLD-A".to_string(), true),
            update,
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
            None,
            TierCandidateMutation::Remove("COLD-A".to_string(), true),
            update,
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
                None,
                TierCandidateMutation::Remove("COLD-A".to_string(), true),
                update,
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while old.is_current(&manager).await {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned update should revoke before caller cancellation");
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
        assert!(store.state.lock().await.is_some(), "detached update must persist its result");
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
            .driver_cache
            .insert("COLD-A".to_string(), Box::new(LeaseTestBackend::panicking_in_use("panic")));
        let update = TierConfigMgr::admin_update_lock(&manager).await;

        let err = TierConfigMgr::update_candidate_owned(
            &manager,
            store,
            candidate,
            None,
            TierCandidateMutation::Remove("COLD-A".to_string(), false),
            update,
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
