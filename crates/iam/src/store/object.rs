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

use super::{GroupInfo, MappedPolicy, Store, UserType};
use crate::error::{Error, Result, is_err_config_not_found, is_err_no_such_group};
use crate::{
    IAM_CONFIG_ROOT_PREFIX, IamStorageError, IamStore, classify_iam_system_path_failure_reason, delete_iam_config,
    read_iam_config_no_lock, read_iam_config_with_metadata, save_iam_config, save_iam_config_with_opts,
};
use crate::{
    cache::{Cache, CacheEntity},
    error::{is_err_no_such_policy, is_err_no_such_user},
    keyring,
    manager::{extract_jwt_claims, extract_jwt_claims_allow_missing_exp, get_default_policyes},
    root_credentials,
};
use futures::future::join_all;
use rustfs_io_metrics::record_system_path_failure;
use rustfs_policy::{auth::UserIdentity, policy::PolicyDoc};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join_buf};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::{self, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::storage_api::object_store::{
    HTTPPreconditions, ListOperations as _, ObjectInfoOrErr as StorageObjectInfoOrErr, ObjectOperations,
};

pub static IAM_CONFIG_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{IAM_CONFIG_ROOT_PREFIX}/iam"));
pub static IAM_CONFIG_USERS_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{IAM_CONFIG_ROOT_PREFIX}/iam/users/"));
pub static IAM_CONFIG_SERVICE_ACCOUNTS_PREFIX: LazyLock<String> =
    LazyLock::new(|| format!("{IAM_CONFIG_ROOT_PREFIX}/iam/service-accounts/"));
pub static IAM_CONFIG_GROUPS_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{IAM_CONFIG_ROOT_PREFIX}/iam/groups/"));
pub static IAM_CONFIG_POLICIES_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{IAM_CONFIG_ROOT_PREFIX}/iam/policies/"));
pub static IAM_CONFIG_STS_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{IAM_CONFIG_ROOT_PREFIX}/iam/sts/"));
pub static IAM_CONFIG_POLICY_DB_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{IAM_CONFIG_ROOT_PREFIX}/iam/policydb/"));
pub static IAM_CONFIG_POLICY_DB_USERS_PREFIX: LazyLock<String> =
    LazyLock::new(|| format!("{IAM_CONFIG_ROOT_PREFIX}/iam/policydb/users/"));
pub static IAM_CONFIG_POLICY_DB_STS_USERS_PREFIX: LazyLock<String> =
    LazyLock::new(|| format!("{IAM_CONFIG_ROOT_PREFIX}/iam/policydb/sts-users/"));
pub static IAM_CONFIG_POLICY_DB_SERVICE_ACCOUNTS_PREFIX: LazyLock<String> =
    LazyLock::new(|| format!("{IAM_CONFIG_ROOT_PREFIX}/iam/policydb/service-accounts/"));
pub static IAM_CONFIG_POLICY_DB_GROUPS_PREFIX: LazyLock<String> =
    LazyLock::new(|| format!("{IAM_CONFIG_ROOT_PREFIX}/iam/policydb/groups/"));

type ObjectInfoOrErr = StorageObjectInfoOrErr<IamObjectInfo, IamStorageError>;
type IamObjectInfo = <IamStore as ObjectOperations>::ObjectInfo;
type IamObjectOptions = <IamStore as ObjectOperations>::ObjectOptions;

const IAM_IDENTITY_FILE: &str = "identity.json";
const IAM_POLICY_FILE: &str = "policy.json";
const IAM_GROUP_MEMBERS_FILE: &str = "members.json";

fn get_user_identity_path(user: &str, user_type: UserType) -> String {
    let base_path: &str = match user_type {
        UserType::Svc => &IAM_CONFIG_SERVICE_ACCOUNTS_PREFIX,
        UserType::Sts => &IAM_CONFIG_STS_PREFIX,
        _ => &IAM_CONFIG_USERS_PREFIX,
    };

    path_join_buf(&[base_path, user, IAM_IDENTITY_FILE])
}

fn get_group_info_path(group: &str) -> String {
    path_join_buf(&[&IAM_CONFIG_GROUPS_PREFIX, group, IAM_GROUP_MEMBERS_FILE])
}

fn get_policy_doc_path(name: &str) -> String {
    path_join_buf(&[&IAM_CONFIG_POLICIES_PREFIX, name, IAM_POLICY_FILE])
}

fn get_mapped_policy_path(name: &str, user_type: UserType, is_group: bool) -> String {
    if is_group {
        return path_join_buf(&[&IAM_CONFIG_POLICY_DB_GROUPS_PREFIX, format!("{name}.json").as_str()]);
    }
    match user_type {
        UserType::Svc => path_join_buf(&[&IAM_CONFIG_POLICY_DB_SERVICE_ACCOUNTS_PREFIX, format!("{name}.json").as_str()]),
        UserType::Sts => path_join_buf(&[&IAM_CONFIG_POLICY_DB_STS_USERS_PREFIX, format!("{name}.json").as_str()]),
        _ => path_join_buf(&[&IAM_CONFIG_POLICY_DB_USERS_PREFIX, format!("{name}.json").as_str()]),
    }
}

#[derive(Debug)]
pub struct StringOrErr {
    pub item: Option<String>,
    pub err: Option<Error>,
}

const USERS_LIST_KEY: &str = "users/";
const SVC_ACC_LIST_KEY: &str = "service-accounts/";
const GROUPS_LIST_KEY: &str = "groups/";
const POLICIES_LIST_KEY: &str = "policies/";
const STS_LIST_KEY: &str = "sts/";
const POLICY_DB_PREFIX: &str = "policydb/";
const POLICY_DB_USERS_LIST_KEY: &str = "policydb/users/";
const POLICY_DB_STS_USERS_LIST_KEY: &str = "policydb/sts-users/";
const POLICY_DB_GROUPS_LIST_KEY: &str = "policydb/groups/";
const IAM_LAZY_REWRITE_COOLDOWN: Duration = Duration::from_secs(60);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DecryptSource {
    Plaintext,
    CurrentMasterKey,
    OldMasterKey,
    LegacySecretKey,
    LegacyAccessSecretKey,
}

#[derive(Debug)]
struct DecryptOutcome {
    plain: Vec<u8>,
    source: DecryptSource,
}

#[derive(Default, Clone, Copy, Debug)]
struct LazyRewriteEntry {
    in_flight: bool,
    cooldown_until: Option<Instant>,
}

static IAM_LAZY_REWRITE_TRACKER: LazyLock<Mutex<HashMap<String, LazyRewriteEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// split_path splits a path into a top-level directory and a child item. The
// parent directory retains the trailing slash.
fn split_path(s: &str, last_index: bool) -> (&str, &str) {
    let i = if last_index { s.rfind('/') } else { s.find('/') };

    match i {
        Some(index) => (&s[..index + 1], &s[index + 1..]),
        None => (s, ""),
    }
}

#[derive(Clone)]
pub struct ObjectStore {
    object_api: Arc<IamStore>,
}

impl ObjectStore {
    const BUCKET_NAME: &'static str = ".rustfs.sys";

    pub fn new(object_api: Arc<IamStore>) -> Self {
        Self { object_api }
    }

    fn decrypt_data_with_source(data: &[u8]) -> Result<DecryptOutcome> {
        if Self::is_plaintext_json(data) {
            return Ok(DecryptOutcome {
                plain: data.to_vec(),
                source: DecryptSource::Plaintext,
            });
        }

        const STREAM_IO_HEADER_LEN: usize = 41;
        let cred = root_credentials::credentials_or_default();
        let mut last_err = None;

        let mut try_decrypt_with_key = |key: &[u8], source: DecryptSource| -> Option<DecryptOutcome> {
            if data.len() >= STREAM_IO_HEADER_LEN
                && let Ok(plain) = rustfs_crypto::decrypt_stream_io(key, data)
            {
                return Some(DecryptOutcome { plain, source });
            }

            match rustfs_crypto::decrypt_data(key, data) {
                Ok(plain) => Some(DecryptOutcome { plain, source }),
                Err(err) => {
                    last_err = Some(err);
                    None
                }
            }
        };

        let (current_key, old_keys) = keyring::current_key_and_old_keys();
        if let Some(key) = current_key
            && let Some(outcome) = try_decrypt_with_key(&key, DecryptSource::CurrentMasterKey)
        {
            return Ok(outcome);
        }

        for key in old_keys {
            if let Some(outcome) = try_decrypt_with_key(&key, DecryptSource::OldMasterKey) {
                return Ok(outcome);
            }
        }

        let secret_key = cred.secret_key;
        let mut legacy_keys = Vec::new();
        if !secret_key.is_empty() {
            legacy_keys.push((secret_key.clone().into_bytes(), DecryptSource::LegacySecretKey));
        }
        if !cred.access_key.is_empty() && !secret_key.is_empty() {
            legacy_keys.push((
                format!("{}:{secret_key}", cred.access_key).into_bytes(),
                DecryptSource::LegacyAccessSecretKey,
            ));
        }

        for (key, source) in legacy_keys {
            if let Some(outcome) = try_decrypt_with_key(&key, source) {
                return Ok(outcome);
            }
        }

        Err(last_err.unwrap_or(rustfs_crypto::Error::ErrUnexpectedHeader).into())
    }

    fn encrypt_data_with_master_key(data: &[u8]) -> Result<Vec<u8>> {
        let Some(master_key) = keyring::encrypt_key() else {
            return Err(Error::other("iam master key is not configured"));
        };

        let encrypted = rustfs_crypto::encrypt_stream_io(&master_key, data)?;
        Ok(encrypted)
    }

    fn prepare_data_for_storage(data: &[u8]) -> Result<Vec<u8>> {
        if keyring::encrypt_key().is_some() {
            let encrypted = Self::encrypt_data_with_master_key(data)?;
            return Ok(encrypted);
        }

        Ok(data.to_vec())
    }

    fn should_lazy_rewrite(source: DecryptSource) -> bool {
        matches!(
            source,
            DecryptSource::Plaintext
                | DecryptSource::OldMasterKey
                | DecryptSource::LegacySecretKey
                | DecryptSource::LegacyAccessSecretKey
        )
    }

    fn begin_lazy_rewrite(path: &str) -> bool {
        let Ok(mut tracker) = IAM_LAZY_REWRITE_TRACKER.lock() else {
            return false;
        };

        let entry = tracker.entry(path.to_owned()).or_default();
        if entry.in_flight {
            return false;
        }
        if entry.cooldown_until.is_some_and(|deadline| deadline > Instant::now()) {
            return false;
        }

        entry.in_flight = true;
        entry.cooldown_until = None;
        true
    }

    fn complete_lazy_rewrite(path: &str, success: bool) {
        let Ok(mut tracker) = IAM_LAZY_REWRITE_TRACKER.lock() else {
            return;
        };

        if success {
            tracker.remove(path);
            return;
        }

        let entry = tracker.entry(path.to_owned()).or_default();
        entry.in_flight = false;
        entry.cooldown_until = Some(Instant::now() + IAM_LAZY_REWRITE_COOLDOWN);
    }

    fn maybe_schedule_lazy_rewrite(&self, path: &str, outcome: &DecryptOutcome, object_info: &IamObjectInfo) {
        if !Self::should_lazy_rewrite(outcome.source) {
            return;
        }
        if keyring::encrypt_key().is_none() {
            return;
        }

        let Some(etag) = object_info.etag.clone() else {
            return;
        };

        if !Self::begin_lazy_rewrite(path) {
            return;
        }

        let path = path.to_owned();
        let plain = outcome.plain.clone();
        let store = self.clone();
        tokio::spawn(async move {
            let result = store.lazy_rewrite_iam_config(path.as_str(), &plain, etag.as_str()).await;
            match result {
                Ok(_) => {
                    Self::complete_lazy_rewrite(path.as_str(), true);
                }
                Err(IamStorageError::PreconditionFailed) => {
                    Self::complete_lazy_rewrite(path.as_str(), false);
                    debug!(path = %path, state = "stale_etag", "IAM lazy rewrite skipped");
                }
                Err(err) => {
                    Self::complete_lazy_rewrite(path.as_str(), false);
                    warn!(path = %path, error = %err, state = "rewrite_failed", "IAM lazy rewrite failed");
                }
            }
        });
    }

    async fn lazy_rewrite_iam_config(&self, path: &str, plain: &[u8], etag: &str) -> std::result::Result<(), IamStorageError> {
        let encrypted = Self::encrypt_data_with_master_key(plain).map_err(IamStorageError::other)?;

        let mut opts = IamObjectOptions {
            max_parity: true,
            ..Default::default()
        };
        opts.http_preconditions = Some(HTTPPreconditions {
            if_match: Some(etag.to_owned()),
            ..Default::default()
        });

        save_iam_config_with_opts(self.object_api.clone(), path, encrypted, &opts).await
    }

    fn is_plaintext_json(data: &[u8]) -> bool {
        std::str::from_utf8(data).is_ok() && serde_json::from_slice::<serde_json::Value>(data).is_ok()
    }

    #[cfg(test)]
    fn prepare_data_for_storage_for_test(data: &[u8]) -> Result<Vec<u8>> {
        Self::prepare_data_for_storage(data)
    }

    async fn load_iamconfig_bytes_with_metadata(&self, path: impl AsRef<str> + Send) -> Result<(Vec<u8>, IamObjectInfo)> {
        let path_ref = path.as_ref();
        let (data, obj) = read_iam_config_with_metadata(self.object_api.clone(), path_ref, &IamObjectOptions::default()).await?;
        let outcome = Self::decrypt_data_with_source(&data)?;
        self.maybe_schedule_lazy_rewrite(path_ref, &outcome, &obj);

        Ok((outcome.plain, obj))
    }

    async fn list_iam_config_items(&self, prefix: &str, ctx: CancellationToken, sender: Sender<StringOrErr>) {
        // debug!("list iam config items, prefix: {}", &prefix);

        let store = self.object_api.clone();

        let (tx, mut rx) = mpsc::channel::<ObjectInfoOrErr>(100);

        let path = prefix.to_owned();
        let sender_on_error = sender.clone();
        tokio::spawn(async move {
            if let Err(err) = store
                .walk(ctx.clone(), Self::BUCKET_NAME, &path, tx, Default::default())
                .await
            {
                let reason = classify_iam_system_path_failure_reason(&err);
                record_system_path_failure("iam_config", "walk", reason);
                error!(
                    path_kind = "iam_config",
                    operation = "walk",
                    reason,
                    bucket = Self::BUCKET_NAME,
                    prefix = %path,
                    error = %err,
                    "IAM config walk failed"
                );
                let _ = sender_on_error
                    .send(StringOrErr {
                        item: None,
                        err: Some(err.into()),
                    })
                    .await;
            }
        });

        let prefix = prefix.to_owned();
        tokio::spawn(async move {
            while let Some(v) = rx.recv().await {
                if let Some(err) = v.err {
                    let _ = sender
                        .send(StringOrErr {
                            item: None,
                            err: Some(err.into()),
                        })
                        .await;
                    return;
                }

                if let Some(info) = v.item {
                    let object_name = if cfg!(target_os = "windows") {
                        info.name.replace('\\', "/")
                    } else {
                        info.name
                    };
                    let name = object_name.trim_start_matches(&prefix).trim_end_matches(SLASH_SEPARATOR);
                    let _ = sender
                        .send(StringOrErr {
                            item: Some(name.to_owned()),
                            err: None,
                        })
                        .await;
                }
            }
        });
    }

    async fn list_all_iamconfig_items(&self) -> Result<HashMap<String, Vec<String>>> {
        let (tx, mut rx) = mpsc::channel::<StringOrErr>(100);

        let ctx = CancellationToken::new();

        self.list_iam_config_items(format!("{}/", *IAM_CONFIG_PREFIX).as_str(), ctx.clone(), tx)
            .await;

        let mut res = HashMap::new();

        while let Some(v) = rx.recv().await {
            if let Some(err) = v.err {
                warn!(
                    path_kind = "iam_config",
                    operation = "list_items",
                    reason = "walk_result",
                    error = %err,
                    "system path list failed"
                );
                record_system_path_failure("iam_config", "list_items", "walk_result");
                ctx.cancel();

                return Err(err);
            }

            if let Some(item) = v.item {
                let last_index = item.starts_with(POLICY_DB_PREFIX);
                let (list_key, trimmed_item) = split_path(&item, last_index);
                res.entry(list_key.to_owned())
                    .or_insert_with(Vec::new)
                    .push(trimmed_item.to_owned());
            }
        }

        ctx.cancel();

        Ok(res)
    }

    async fn load_policy_doc_concurrent(&self, names: &[String]) -> Result<Vec<PolicyDoc>> {
        let mut futures = Vec::with_capacity(names.len());

        for name in names {
            let policy_name = rustfs_utils::path::dir(name);
            futures.push(async move {
                match self.load_policy(&policy_name).await {
                    Ok(p) => Ok(p),
                    Err(err) => {
                        if !is_err_no_such_policy(&err) {
                            Err(Error::other(format!("load policy doc failed: {err}")))
                        } else {
                            Ok(PolicyDoc::default())
                        }
                    }
                }
            });
        }

        let results = join_all(futures).await;

        let mut policies = Vec::with_capacity(results.len());
        for r in results {
            match r {
                Ok(p) => policies.push(p),
                Err(e) => return Err(e),
            }
        }

        Ok(policies)
    }

    async fn load_user_concurrent(&self, names: &[String], user_type: UserType) -> Result<Vec<UserIdentity>> {
        let mut futures = Vec::with_capacity(names.len());

        for name in names {
            let user_name = rustfs_utils::path::dir(name);
            futures.push(async move {
                match self.load_user_identity(&user_name, user_type).await {
                    Ok(res) => Ok(res),
                    Err(err) => {
                        if !is_err_no_such_user(&err) {
                            Err(Error::other(format!("load user failed: {err}")))
                        } else {
                            Ok(UserIdentity::default())
                        }
                    }
                }
            });
        }

        let results = join_all(futures).await;

        let mut users = Vec::with_capacity(results.len());
        for r in results {
            match r {
                Ok(u) => users.push(u),
                Err(e) => return Err(e),
            }
        }
        Ok(users)
    }

    async fn load_mapped_policy_internal(&self, name: &str, user_type: UserType, is_group: bool) -> Result<MappedPolicy> {
        let info: MappedPolicy = self
            .load_iam_config(get_mapped_policy_path(name, user_type, is_group))
            .await
            .map_err(|err| {
                if is_err_config_not_found(&err) {
                    Error::NoSuchPolicy
                } else {
                    err
                }
            })?;

        Ok(info)
    }

    async fn load_mapped_policy_concurrent(
        &self,
        names: &[String],
        user_type: UserType,
        is_group: bool,
    ) -> Result<Vec<MappedPolicy>> {
        let mut futures = Vec::with_capacity(names.len());

        for name in names {
            let policy_name = name.trim_end_matches(".json");
            futures.push(async move {
                match self.load_mapped_policy_internal(policy_name, user_type, is_group).await {
                    Ok(p) => Ok(p),
                    Err(err) => {
                        if !is_err_no_such_policy(&err) {
                            Err(Error::other(format!("load mapped policy failed: {err}")))
                        } else {
                            Ok(MappedPolicy::default())
                        }
                    }
                }
            });
        }

        let results = join_all(futures).await;

        let mut policies = Vec::with_capacity(results.len());
        for r in results {
            match r {
                Ok(p) => policies.push(p),
                Err(e) => return Err(e),
            }
        }

        Ok(policies)
    }

    /// Checks if the underlying ECStore is ready for metadata operations.
    /// This prevents silent failures during the storage boot-up phase.
    ///
    /// Performs a lightweight probe by attempting to read a known configuration object.
    /// If the object is not found, it indicates the storage metadata is not ready.
    /// The upper-level caller should handle retries if needed.
    async fn check_storage_readiness(&self) -> Result<()> {
        // Probe path for a fixed object under the IAM root prefix.
        // If it doesn't exist, the system bucket or metadata is not ready.
        let probe_path = format!("{}/format.json", *IAM_CONFIG_PREFIX);

        match read_iam_config_no_lock(self.object_api.clone(), &probe_path).await {
            Ok(_) => Ok(()),
            Err(IamStorageError::ConfigNotFound) => Err(Error::other(format!(
                "Storage metadata not ready: probe object '{}' not found (expected IAM config to be initialized)",
                probe_path
            ))),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait::async_trait]
impl Store for ObjectStore {
    fn has_watcher(&self) -> bool {
        false
    }
    async fn load_iam_config<Item: DeserializeOwned>(&self, path: impl AsRef<str> + Send) -> Result<Item> {
        let path_ref = path.as_ref();
        let (data, obj) = read_iam_config_with_metadata(self.object_api.clone(), path_ref, &IamObjectOptions::default()).await?;

        let outcome = match Self::decrypt_data_with_source(&data) {
            Ok(v) => v,
            Err(err) => {
                warn!(path = %path_ref, error = %err, "IAM config decrypt failed; keeping file");
                // keep the config file when decrypt failed - do not delete
                return Err(Error::ConfigNotFound);
            }
        };

        self.maybe_schedule_lazy_rewrite(path_ref, &outcome, &obj);

        Ok(serde_json::from_slice(&outcome.plain)?)
    }
    /// Saves IAM configuration with a retry mechanism on failure.
    ///
    /// Attempts to save the IAM configuration up to 5 times if the storage layer is not ready,
    /// using exponential backoff between attempts (starting at 200ms, doubling each retry).
    ///
    /// # Arguments
    ///
    /// * `item` - The IAM configuration item to save, must implement `Serialize` and `Send`.
    /// * `path` - The path where the configuration will be saved.
    ///
    /// # Returns
    ///
    /// * `Result<()>` - `Ok(())` on success, or an `Error` if all attempts fail.
    #[tracing::instrument(skip(self, item, path))]
    async fn save_iam_config<Item: Serialize + Send>(&self, item: Item, path: impl AsRef<str> + Send) -> Result<()> {
        let mut data = serde_json::to_vec(&item)?;
        data = Self::prepare_data_for_storage(&data)?;

        let mut attempts = 0;
        let max_attempts = 5;
        let path_ref = path.as_ref();

        loop {
            match save_iam_config(self.object_api.clone(), path_ref, data.clone()).await {
                Ok(_) => {
                    debug!("Successfully saved IAM config to {}", path_ref);
                    return Ok(());
                }
                Err(e) if attempts < max_attempts => {
                    attempts += 1;
                    // Exponential backoff: 200ms, 400ms, 800ms...
                    let wait_ms = 200 * (1 << attempts);
                    warn!(
                        "Storage layer not ready for IAM write (attempt {}/{}). Retrying in {}ms. Path: {}, Error: {:?}",
                        attempts, max_attempts, wait_ms, path_ref, e
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(wait_ms)).await;
                }
                Err(e) => {
                    error!(path = %path_ref, error = ?e, "IAM config save failed");
                    return Err(e.into());
                }
            }
        }
    }
    async fn delete_iam_config(&self, path: impl AsRef<str> + Send) -> Result<()> {
        delete_iam_config(self.object_api.clone(), path.as_ref()).await?;
        Ok(())
    }

    async fn save_user_identity(
        &self,
        name: &str,
        user_type: UserType,
        user_identity: UserIdentity,
        _ttl: Option<usize>,
    ) -> Result<()> {
        // Pre-check storage health
        self.check_storage_readiness().await?;

        let path = get_user_identity_path(name, user_type);
        debug!("Saving IAM identity to path: {}", path);

        self.save_iam_config(user_identity, path).await.map_err(|e| {
            error!(name, error = ?e, "IAM identity save failed");
            e
        })
    }
    async fn delete_user_identity(&self, name: &str, user_type: UserType) -> Result<()> {
        self.delete_iam_config(get_user_identity_path(name, user_type))
            .await
            .map_err(|err| {
                if is_err_config_not_found(&err) {
                    Error::NoSuchPolicy
                } else {
                    err
                }
            })?;
        Ok(())
    }
    async fn load_user_identity(&self, name: &str, user_type: UserType) -> Result<UserIdentity> {
        let mut u: UserIdentity = self
            .load_iam_config(get_user_identity_path(name, user_type))
            .await
            .map_err(|err| {
                if is_err_config_not_found(&err) {
                    warn!(name, user_type = ?user_type, "IAM user identity missing");
                    Error::NoSuchUser(name.to_owned())
                } else {
                    warn!(name, user_type = ?user_type, error = ?err, "IAM user identity load failed");
                    err
                }
            })?;

        if u.credentials.is_expired() {
            let _ = self.delete_iam_config(get_user_identity_path(name, user_type)).await;
            let _ = self.delete_iam_config(get_mapped_policy_path(name, user_type, false)).await;
            warn!(name, user_type = ?user_type, "IAM user identity expired and was removed");
            return Err(Error::NoSuchUser(name.to_owned()));
        }

        if u.credentials.access_key.is_empty() {
            u.credentials.access_key = name.to_owned();
        }

        if !u.credentials.session_token.is_empty() {
            let claims_result = if user_type == UserType::Svc && u.credentials.expiration.is_none() {
                extract_jwt_claims_allow_missing_exp(&u)
            } else {
                extract_jwt_claims(&u)
            };

            match claims_result {
                Ok(claims) => {
                    u.credentials.claims = Some(claims);
                }
                Err(err) => {
                    if u.credentials.is_temp() {
                        let _ = self.delete_iam_config(get_user_identity_path(name, user_type)).await;
                        let _ = self.delete_iam_config(get_mapped_policy_path(name, user_type, false)).await;
                    }
                    warn!(name, user_type = ?user_type, error = ?err, "IAM JWT claim extraction failed");
                    return Err(Error::NoSuchUser(name.to_owned()));
                }
            }
        }

        Ok(u)
    }
    async fn load_user(&self, name: &str, user_type: UserType, m: &mut HashMap<String, UserIdentity>) -> Result<()> {
        self.load_user_identity(name, user_type).await.map(|u| {
            m.insert(name.to_owned(), u);
        })
    }
    async fn load_users(&self, user_type: UserType, m: &mut HashMap<String, UserIdentity>) -> Result<()> {
        let base_prefix = match user_type {
            UserType::Reg => IAM_CONFIG_USERS_PREFIX.as_str(),
            UserType::Svc => IAM_CONFIG_SERVICE_ACCOUNTS_PREFIX.as_str(),
            UserType::Sts => IAM_CONFIG_STS_PREFIX.as_str(),
            UserType::None => "",
        };

        let ctx = CancellationToken::new();
        let (tx, mut rx) = mpsc::channel::<StringOrErr>(100);

        self.list_iam_config_items(base_prefix, ctx.clone(), tx).await;

        while let Some(v) = rx.recv().await {
            if let Some(err) = v.err {
                warn!(error = ?err, "IAM config item listing failed");
                let _ = ctx.cancel();

                return Err(err);
            }

            if let Some(item) = v.item {
                let name = rustfs_utils::path::dir(&item);
                self.load_user(&name, user_type, m).await?;
            }
        }
        let _ = ctx.cancel();
        Ok(())
    }
    async fn load_secret_key(&self, name: &str, user_type: UserType) -> Result<String> {
        let u: UserIdentity = self
            .load_iam_config(get_user_identity_path(name, user_type))
            .await
            .map_err(|err| {
                if is_err_config_not_found(&err) {
                    Error::NoSuchUser(name.to_owned())
                } else {
                    err
                }
            })?;

        Ok(u.credentials.secret_key)
    }

    async fn save_group_info(&self, name: &str, item: GroupInfo) -> Result<()> {
        self.save_iam_config(item, get_group_info_path(name)).await
    }
    async fn delete_group_info(&self, name: &str) -> Result<()> {
        self.delete_iam_config(get_group_info_path(name)).await.map_err(|err| {
            if is_err_config_not_found(&err) {
                Error::NoSuchGroup(name.to_string())
            } else {
                err
            }
        })?;
        Ok(())
    }
    async fn load_group(&self, name: &str, m: &mut HashMap<String, GroupInfo>) -> Result<()> {
        let u: GroupInfo = self.load_iam_config(get_group_info_path(name)).await.map_err(|err| {
            if is_err_config_not_found(&err) {
                Error::NoSuchGroup(name.to_string())
            } else {
                err
            }
        })?;

        m.insert(name.to_owned(), u);
        Ok(())
    }
    async fn load_groups(&self, m: &mut HashMap<String, GroupInfo>) -> Result<()> {
        let ctx = CancellationToken::new();
        let (tx, mut rx) = mpsc::channel::<StringOrErr>(100);

        self.list_iam_config_items(&IAM_CONFIG_GROUPS_PREFIX, ctx.clone(), tx).await;

        while let Some(v) = rx.recv().await {
            if let Some(err) = v.err {
                warn!(error = ?err, "IAM config item listing failed");
                let _ = ctx.cancel();

                return Err(err);
            }

            if let Some(item) = v.item {
                let name = rustfs_utils::path::dir(&item);
                if let Err(err) = self.load_group(&name, m).await
                    && !is_err_no_such_group(&err)
                {
                    return Err(err);
                }
            }
        }
        let _ = ctx.cancel();
        Ok(())
    }

    async fn save_policy_doc(&self, name: &str, policy_doc: PolicyDoc) -> Result<()> {
        self.save_iam_config(policy_doc, get_policy_doc_path(name)).await
    }
    async fn delete_policy_doc(&self, name: &str) -> Result<()> {
        self.delete_iam_config(get_policy_doc_path(name)).await.map_err(|err| {
            if is_err_config_not_found(&err) {
                Error::NoSuchPolicy
            } else {
                err
            }
        })?;
        Ok(())
    }
    async fn load_policy(&self, name: &str) -> Result<PolicyDoc> {
        let (data, obj) = self
            .load_iamconfig_bytes_with_metadata(get_policy_doc_path(name))
            .await
            .map_err(|err| {
                if is_err_config_not_found(&err) {
                    Error::NoSuchPolicy
                } else {
                    err
                }
            })?;

        let mut info = PolicyDoc::try_from(data)?;

        if info.version == 0 {
            info.create_date = obj.mod_time;
            info.update_date = obj.mod_time;
        }

        Ok(info)
    }

    async fn load_policy_doc(&self, name: &str, m: &mut HashMap<String, PolicyDoc>) -> Result<()> {
        let info = self.load_policy(name).await?;
        m.insert(name.to_owned(), info);

        Ok(())
    }
    async fn load_policy_docs(&self, m: &mut HashMap<String, PolicyDoc>) -> Result<()> {
        let ctx = CancellationToken::new();
        let (tx, mut rx) = mpsc::channel::<StringOrErr>(100);

        self.list_iam_config_items(&IAM_CONFIG_POLICIES_PREFIX, ctx.clone(), tx).await;

        while let Some(v) = rx.recv().await {
            if let Some(err) = v.err {
                warn!(error = ?err, "IAM config item listing failed");
                let _ = ctx.cancel();

                return Err(err);
            }

            if let Some(item) = v.item {
                let name = rustfs_utils::path::dir(&item);
                self.load_policy_doc(&name, m).await?;
            }
        }
        let _ = ctx.cancel();
        Ok(())
    }

    async fn save_mapped_policy(
        &self,
        name: &str,
        user_type: UserType,
        is_group: bool,
        mapped_policy: MappedPolicy,
        _ttl: Option<usize>,
    ) -> Result<()> {
        self.save_iam_config(mapped_policy, get_mapped_policy_path(name, user_type, is_group))
            .await
    }
    async fn delete_mapped_policy(&self, name: &str, user_type: UserType, is_group: bool) -> Result<()> {
        self.delete_iam_config(get_mapped_policy_path(name, user_type, is_group))
            .await
            .map_err(|err| {
                if is_err_config_not_found(&err) {
                    Error::NoSuchPolicy
                } else {
                    err
                }
            })?;
        Ok(())
    }
    async fn load_mapped_policy(
        &self,
        name: &str,
        user_type: UserType,
        is_group: bool,
        m: &mut HashMap<String, MappedPolicy>,
    ) -> Result<()> {
        let info = self.load_mapped_policy_internal(name, user_type, is_group).await?;

        m.insert(name.to_owned(), info);

        Ok(())
    }
    async fn load_mapped_policies(
        &self,
        user_type: UserType,
        is_group: bool,
        m: &mut HashMap<String, MappedPolicy>,
    ) -> Result<()> {
        let base_path = {
            if is_group {
                IAM_CONFIG_POLICY_DB_GROUPS_PREFIX.as_str()
            } else {
                match user_type {
                    UserType::Svc => IAM_CONFIG_POLICY_DB_SERVICE_ACCOUNTS_PREFIX.as_str(),
                    UserType::Sts => IAM_CONFIG_POLICY_DB_STS_USERS_PREFIX.as_str(),
                    _ => IAM_CONFIG_POLICY_DB_USERS_PREFIX.as_str(),
                }
            }
        };
        let ctx = CancellationToken::new();
        let (tx, mut rx) = mpsc::channel::<StringOrErr>(100);

        self.list_iam_config_items(base_path, ctx.clone(), tx).await;

        while let Some(v) = rx.recv().await {
            if let Some(err) = v.err {
                warn!(error = ?err, "IAM config item listing failed");
                let _ = ctx.cancel();

                return Err(err);
            }

            if let Some(item) = v.item {
                let name = item.trim_end_matches(".json");
                self.load_mapped_policy(name, user_type, is_group, m).await?;
            }
        }
        let _ = ctx.cancel(); // TODO: check if this is needed
        Ok(())
    }

    async fn load_all(&self, cache: &Cache) -> Result<()> {
        let cache_snapshot = cache.snapshot();
        let listed_config_items = self.list_all_iamconfig_items().await?;

        let mut policy_docs_cache = CacheEntity::new(get_default_policyes());

        if let Some(policies_list) = listed_config_items.get(POLICIES_LIST_KEY) {
            let mut policies_list = policies_list.clone();

            loop {
                if policies_list.len() < 32 {
                    let policy_docs = self.load_policy_doc_concurrent(&policies_list).await?;

                    for (idx, p) in policy_docs.into_iter().enumerate() {
                        if p.policy.version.is_empty() {
                            continue;
                        }

                        let policy_name = rustfs_utils::path::dir(&policies_list[idx]);

                        debug!(policy = %policy_name, "IAM policy loaded");

                        policy_docs_cache.insert(policy_name, p);
                    }
                    break;
                }

                let policy_docs = self.load_policy_doc_concurrent(&policies_list).await?;

                for (idx, p) in policy_docs.into_iter().enumerate() {
                    if p.policy.version.is_empty() {
                        continue;
                    }

                    let policy_name = rustfs_utils::path::dir(&policies_list[idx]);
                    debug!(policy = %policy_name, "IAM policy loaded");
                    policy_docs_cache.insert(policy_name, p);
                }

                policies_list = policies_list.split_off(32);
            }
        }

        let mut user_items_cache = CacheEntity::default();

        // users
        if let Some(item_name_list) = listed_config_items.get(USERS_LIST_KEY) {
            let mut item_name_list = item_name_list.clone();

            loop {
                if item_name_list.len() < 32 {
                    let items = self.load_user_concurrent(&item_name_list, UserType::Reg).await?;

                    for (idx, p) in items.into_iter().enumerate() {
                        if p.credentials.access_key.is_empty() {
                            continue;
                        }

                        let name = rustfs_utils::path::dir(&item_name_list[idx]);
                        debug!(user = %name, "IAM regular user loaded");
                        user_items_cache.insert(name, p);
                    }
                    break;
                }

                let items = self.load_user_concurrent(&item_name_list, UserType::Reg).await?;

                for (idx, p) in items.into_iter().enumerate() {
                    if p.credentials.access_key.is_empty() {
                        continue;
                    }

                    let name = rustfs_utils::path::dir(&item_name_list[idx]);
                    debug!(user = %name, "IAM regular user loaded");
                    user_items_cache.insert(name, p);
                }

                item_name_list = item_name_list.split_off(32);
            }
        }

        // groups
        let mut groups_cache = None;
        if let Some(item_name_list) = listed_config_items.get(GROUPS_LIST_KEY) {
            let mut items_cache = CacheEntity::default();

            for item in item_name_list.iter() {
                let name = rustfs_utils::path::dir(item);
                debug!(group = %name, "IAM group loaded");
                if let Err(err) = self.load_group(&name, &mut items_cache).await {
                    return Err(Error::other(format!("load group failed: {err}")));
                };
            }

            groups_cache = Some(items_cache);
        }

        // user policies
        let mut user_policies_cache = None;
        if let Some(item_name_list) = listed_config_items.get(POLICY_DB_USERS_LIST_KEY) {
            let mut item_name_list = item_name_list.clone();

            let mut items_cache = CacheEntity::default();

            loop {
                if item_name_list.len() < 32 {
                    let items = self
                        .load_mapped_policy_concurrent(&item_name_list, UserType::Reg, false)
                        .await?;

                    for (idx, p) in items.into_iter().enumerate() {
                        if p.policies.is_empty() {
                            continue;
                        }

                        let name = item_name_list[idx].trim_end_matches(".json").to_owned();
                        debug!(user = %name, "IAM user policy loaded");
                        items_cache.insert(name, p);
                    }
                    break;
                }

                let items = self
                    .load_mapped_policy_concurrent(&item_name_list, UserType::Reg, false)
                    .await?;

                for (idx, p) in items.into_iter().enumerate() {
                    if p.policies.is_empty() {
                        continue;
                    }

                    let name = item_name_list[idx].trim_end_matches(".json").to_owned();
                    debug!(user = %name, "IAM user policy loaded");
                    items_cache.insert(name, p);
                }

                item_name_list = item_name_list.split_off(32);
            }

            user_policies_cache = Some(items_cache);
        }

        // group policy
        let mut group_policies_cache = None;
        if let Some(item_name_list) = listed_config_items.get(POLICY_DB_GROUPS_LIST_KEY) {
            let mut items_cache = CacheEntity::default();

            for item in item_name_list.iter() {
                let name = item.trim_end_matches(".json");

                debug!(group = %name, "IAM group policy loaded");
                if let Err(err) = self.load_mapped_policy(name, UserType::Reg, true, &mut items_cache).await
                    && !is_err_no_such_policy(&err)
                {
                    return Err(Error::other(format!("load group policy failed: {err}")));
                };
            }

            group_policies_cache = Some(items_cache);
        }

        let mut sts_policies_cache = CacheEntity::default();

        // svc users
        if let Some(item_name_list) = listed_config_items.get(SVC_ACC_LIST_KEY) {
            let mut items_cache = HashMap::default();

            for item in item_name_list.iter() {
                let name = rustfs_utils::path::dir(item);
                debug!(user = %name, "IAM service user loaded");
                if let Err(err) = self.load_user(&name, UserType::Svc, &mut items_cache).await
                    && !is_err_no_such_user(&err)
                {
                    return Err(Error::other(format!("load svc user failed: {err}")));
                };
            }

            for (_, v) in items_cache.iter() {
                let parent = v.credentials.parent_user.clone();
                if !user_items_cache.contains_key(&parent) {
                    debug!(user = %parent, "IAM STS parent policy loaded");
                    if let Err(err) = self
                        .load_mapped_policy(&parent, UserType::Sts, false, &mut sts_policies_cache)
                        .await
                        && !is_err_no_such_policy(&err)
                    {
                        return Err(Error::other(format!("load_mapped_policy failed: {err}")));
                    }
                }
            }

            // Merge items_cache to user_items_cache
            user_items_cache.extend(items_cache);
        }

        let mut sts_items_cache = CacheEntity::default();
        // sts users
        if let Some(item_name_list) = listed_config_items.get(STS_LIST_KEY) {
            for item in item_name_list.iter() {
                debug!(path = %item, "IAM STS user path discovered");

                let name = rustfs_utils::path::dir(item);
                debug!(user = %name, "IAM STS user loaded");
                if let Err(err) = self.load_user(&name, UserType::Sts, &mut sts_items_cache).await {
                    debug!(user = %name, error = %err, "IAM STS user load failed");
                };
            }
        }

        // sts user policy
        if let Some(item_name_list) = listed_config_items.get(POLICY_DB_STS_USERS_LIST_KEY) {
            for item in item_name_list.iter() {
                let name = item.trim_end_matches(".json");
                debug!(user = %name, "IAM STS user policy loaded");
                if let Err(err) = self
                    .load_mapped_policy(name, UserType::Sts, false, &mut sts_policies_cache)
                    .await
                {
                    debug!(user = %name, error = %err, "IAM STS user policy load failed");
                };
            }
        }

        cache.with_write_lock(|cache| {
            if cache.matches_snapshot(&cache_snapshot) {
                cache.replace_policy_docs(policy_docs_cache);
                if let Some(groups_cache) = groups_cache {
                    cache.replace_groups(groups_cache);
                }
                if let Some(user_policies_cache) = user_policies_cache {
                    cache.replace_user_policies(user_policies_cache);
                }
                if let Some(group_policies_cache) = group_policies_cache {
                    cache.replace_group_policies(group_policies_cache);
                }
                cache.replace_users(user_items_cache);
                cache.replace_sts_accounts(sts_items_cache);
                cache.replace_sts_policies(sts_policies_cache);
                cache.build_user_group_memberships();
            } else {
                warn!("IAM full reload cache commit skipped due to concurrent cache changes");
            }
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{DecryptSource, ObjectStore};
    use crate::keyring;
    use rustfs_credentials::{Credentials, init_global_action_credentials};
    use serial_test::serial;
    use temp_env::with_vars;

    fn test_cred() -> Credentials {
        if let Some(cred) = crate::root_credentials::credentials() {
            return cred;
        }
        let _ = init_global_action_credentials(Some("COMPATTESTAK".to_string()), Some("COMPATTESTSK1234567890".to_string()));
        crate::root_credentials::credentials_or_default()
    }

    #[test]
    fn test_decrypt_data_accepts_plaintext_json() {
        let raw = br#"{"Version":1,"policy":"readonly"}"#;
        let out = ObjectStore::decrypt_data_with_source(raw).expect("plaintext json should pass through");
        assert_eq!(out.plain, raw);
        assert_eq!(out.source, DecryptSource::Plaintext);
    }

    #[test]
    fn test_decrypt_data_accepts_rustfs_legacy_secret_encryption() {
        let cred = test_cred();
        let plain = br#"{"accessKey":"ak","secretKey":"sk"}"#;
        let encrypted = rustfs_crypto::encrypt_data(cred.secret_key.as_bytes(), plain).expect("encrypt with rustfs secret");
        let out = ObjectStore::decrypt_data_with_source(&encrypted).expect("decrypt rustfs legacy encryption");
        assert_eq!(out.plain, plain);
    }

    #[test]
    fn test_decrypt_data_accepts_access_secret_encryption() {
        let cred = test_cred();
        let plain = br#"{"Version":1,"updatedAt":"2025-03-07T12:00:00Z"}"#;
        let root_cred = format!("{}:{}", cred.access_key, cred.secret_key);
        let encrypted = rustfs_crypto::encrypt_stream_io(root_cred.as_bytes(), plain).expect("encrypt with stream_io");
        let out = ObjectStore::decrypt_data_with_source(&encrypted).expect("decrypt stream_io");
        assert_eq!(out.plain, plain);
    }

    #[test]
    fn test_decrypt_data_corrupt_stream_io_fails() {
        let cred = test_cred();
        let plain = br#"{"Version":1}"#;
        let root_cred = format!("{}:{}", cred.access_key, cred.secret_key);
        let mut encrypted = rustfs_crypto::encrypt_stream_io(root_cred.as_bytes(), plain).expect("encrypt with stream_io");
        if encrypted.len() > 50 {
            encrypted[50] ^= 0xFF; // corrupt one byte
        }
        let result = ObjectStore::decrypt_data_with_source(&encrypted);
        assert!(result.is_err(), "corrupt stream_io data should fail decrypt");
    }

    #[test]
    fn test_decrypt_data_short_data_fails() {
        let short = &[0x00u8; 40]; // less than 41-byte stream_io header, not valid JSON
        let result = ObjectStore::decrypt_data_with_source(short);
        assert!(result.is_err(), "short non-JSON data should fail decrypt");
    }

    #[test]
    #[serial]
    fn test_prepare_data_defaults_to_plaintext_without_iam_master_key() {
        let plain = br#"{"Version":1,"policy":"readonly"}"#;

        with_vars(
            [
                (keyring::ENV_IAM_MASTER_KEY, None::<&str>),
                (keyring::ENV_IAM_MASTER_KEY_OLD_KEYS, None::<&str>),
            ],
            || {
                let stored = ObjectStore::prepare_data_for_storage_for_test(plain).expect("store bytes should build");
                assert_eq!(stored, plain);

                let decrypted = ObjectStore::decrypt_data_with_source(&stored).expect("plaintext should load");
                assert_eq!(plain, decrypted.plain.as_slice());
                assert_eq!(decrypted.source, DecryptSource::Plaintext);
            },
        );
    }

    #[test]
    #[serial]
    fn test_prepare_data_uses_iam_master_key_roundtrip() {
        let _ = test_cred();
        let plain = br#"{"Version":1,"policy":"master-key"}"#;
        let master_key = "iam-master-key-roundtrip";

        with_vars(
            [
                (keyring::ENV_IAM_MASTER_KEY, Some(master_key)),
                (keyring::ENV_IAM_MASTER_KEY_OLD_KEYS, None),
            ],
            || {
                let encrypted = ObjectStore::prepare_data_for_storage_for_test(plain).expect("encrypt with iam master key");

                let by_master =
                    rustfs_crypto::decrypt_stream_io(master_key.as_bytes(), &encrypted).expect("decrypt via master key");
                assert_eq!(by_master, plain);

                let by_object_store = ObjectStore::decrypt_data_with_source(&encrypted).expect("decrypt via object store");
                assert_eq!(by_object_store.plain, plain);
                assert_eq!(by_object_store.source, DecryptSource::CurrentMasterKey);
            },
        );
    }

    #[test]
    #[serial]
    fn test_decrypt_data_uses_iam_old_keys_fallback_during_rotation() {
        let _ = test_cred();
        let plain = br#"{"Version":1,"policy":"rotation-fallback"}"#;
        let current_key = "iam-master-key-new";
        let old_key_used_for_data = "iam-master-key-old-2";
        let old_keys = format!("iam-master-key-old-1,{old_key_used_for_data}");

        let encrypted = rustfs_crypto::encrypt_stream_io(old_key_used_for_data.as_bytes(), plain)
            .expect("encrypt with old iam key for rotation simulation");

        assert!(
            rustfs_crypto::decrypt_stream_io(current_key.as_bytes(), &encrypted).is_err(),
            "current master key should not decrypt old-key encrypted data in this test"
        );

        with_vars(
            [
                (keyring::ENV_IAM_MASTER_KEY, Some(current_key)),
                (keyring::ENV_IAM_MASTER_KEY_OLD_KEYS, Some(old_keys.as_str())),
            ],
            || {
                let decrypted = ObjectStore::decrypt_data_with_source(&encrypted).expect("decrypt via old-key fallback");
                assert_eq!(decrypted.plain, plain);
                assert_eq!(decrypted.source, DecryptSource::OldMasterKey);
            },
        );
    }
}
