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
    cache::{Cache, CacheEntity},
    error::{is_err_no_such_policy, is_err_no_such_user},
    manager::{extract_jwt_claims, get_default_policyes},
};
use futures::future::join_all;
use rustfs_credentials::get_global_action_cred;
use rustfs_ecstore::StorageAPI as _;
use rustfs_ecstore::store_api::{ObjectInfoOrErr, WalkOptions};
use rustfs_ecstore::{
    config::{
        RUSTFS_CONFIG_PREFIX,
        com::{delete_config, read_config, read_config_with_metadata, save_config},
    },
    store::ECStore,
    store_api::{ObjectInfo, ObjectOptions},
};
use rustfs_policy::{auth::UserIdentity, policy::PolicyDoc};
use rustfs_utils::path::{SLASH_SEPARATOR, path_join_buf};
use serde::{Serialize, de::DeserializeOwned};
use std::sync::LazyLock;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::mpsc::{self, Sender};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub static IAM_CONFIG_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{RUSTFS_CONFIG_PREFIX}/iam"));
pub static IAM_CONFIG_USERS_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{RUSTFS_CONFIG_PREFIX}/iam/users/"));
pub static IAM_CONFIG_SERVICE_ACCOUNTS_PREFIX: LazyLock<String> =
    LazyLock::new(|| format!("{RUSTFS_CONFIG_PREFIX}/iam/service-accounts/"));
pub static IAM_CONFIG_GROUPS_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{RUSTFS_CONFIG_PREFIX}/iam/groups/"));
pub static IAM_CONFIG_POLICIES_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{RUSTFS_CONFIG_PREFIX}/iam/policies/"));
pub static IAM_CONFIG_STS_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{RUSTFS_CONFIG_PREFIX}/iam/sts/"));
pub static IAM_CONFIG_POLICY_DB_PREFIX: LazyLock<String> = LazyLock::new(|| format!("{RUSTFS_CONFIG_PREFIX}/iam/policydb/"));
pub static IAM_CONFIG_POLICY_DB_USERS_PREFIX: LazyLock<String> =
    LazyLock::new(|| format!("{RUSTFS_CONFIG_PREFIX}/iam/policydb/users/"));
pub static IAM_CONFIG_POLICY_DB_STS_USERS_PREFIX: LazyLock<String> =
    LazyLock::new(|| format!("{RUSTFS_CONFIG_PREFIX}/iam/policydb/sts-users/"));
pub static IAM_CONFIG_POLICY_DB_SERVICE_ACCOUNTS_PREFIX: LazyLock<String> =
    LazyLock::new(|| format!("{RUSTFS_CONFIG_PREFIX}/iam/policydb/service-accounts/"));
pub static IAM_CONFIG_POLICY_DB_GROUPS_PREFIX: LazyLock<String> =
    LazyLock::new(|| format!("{RUSTFS_CONFIG_PREFIX}/iam/policydb/groups/"));

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
    object_api: Arc<ECStore>,
}

impl ObjectStore {
    const BUCKET_NAME: &'static str = ".rustfs.sys";

    pub fn new(object_api: Arc<ECStore>) -> Self {
        Self { object_api }
    }

    fn decrypt_data(data: &[u8]) -> Result<Vec<u8>> {
        let de = rustfs_crypto::decrypt_data(get_global_action_cred().unwrap_or_default().secret_key.as_bytes(), data)?;
        Ok(de)
    }

    fn encrypt_data(data: &[u8]) -> Result<Vec<u8>> {
        let en = rustfs_crypto::encrypt_data(get_global_action_cred().unwrap_or_default().secret_key.as_bytes(), data)?;
        Ok(en)
    }

    async fn load_iamconfig_bytes_with_metadata(&self, path: impl AsRef<str> + Send) -> Result<(Vec<u8>, ObjectInfo)> {
        let (data, obj) = read_config_with_metadata(self.object_api.clone(), path.as_ref(), &ObjectOptions::default()).await?;

        Ok((Self::decrypt_data(&data)?, obj))
    }

    async fn list_iam_config_items(&self, prefix: &str, ctx: CancellationToken, sender: Sender<StringOrErr>) {
        // debug!("list iam config items, prefix: {}", &prefix);

        // TODO: Implement walk, use walk

        // let prefix = format!("{}{}", prefix, item);

        let store = self.object_api.clone();

        let (tx, mut rx) = mpsc::channel::<ObjectInfoOrErr>(100);

        let path = prefix.to_owned();
        tokio::spawn(async move {
            store
                .walk(ctx.clone(), Self::BUCKET_NAME, &path, tx, WalkOptions::default())
                .await
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
                warn!("list_iam_config_items {:?}", err);
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

        match read_config(self.object_api.clone(), &probe_path).await {
            Ok(_) => Ok(()),
            Err(rustfs_ecstore::error::StorageError::ConfigNotFound) => Err(Error::other(format!(
                "Storage metadata not ready: probe object '{}' not found (expected IAM config to be initialized)",
                probe_path
            ))),
            Err(e) => Err(e.into()),
        }
    }

    // async fn load_policy(&self, name: &str) -> Result<PolicyDoc> {
    //     let mut policy = self
    //         .load_iam_config::<PolicyDoc>(&format!("config/iam/policies/{name}/policy.json"))
    //         .await?;

    //     // FIXME:
    //     // if policy.version == 0 {
    //     //     policy.create_date = object.mod_time;
    //     //     policy.update_date = object.mod_time;
    //     // }

    //     Ok(policy)
    // }

    // async fn load_user_identity(&self, user_type: UserType, name: &str) -> Result<Option<UserIdentity>> {
    //     let mut user = self
    //         .load_iam_config::<UserIdentity>(&format!(
    //             "config/iam/{base}{name}/identity.json",
    //             base = user_type.prefix(),
    //             name = name
    //         ))
    //         .await?;

    //     if user.credentials.is_expired() {
    //         return Ok(None);
    //     }

    //     if user.credentials.access_key.is_empty() {
    //         user.credentials.access_key = name.to_owned();
    //     }

    //     // todo, validate session token

    //     Ok(Some(user))
    // }
}

#[async_trait::async_trait]
impl Store for ObjectStore {
    fn has_watcher(&self) -> bool {
        false
    }
    async fn load_iam_config<Item: DeserializeOwned>(&self, path: impl AsRef<str> + Send) -> Result<Item> {
        let mut data = read_config(self.object_api.clone(), path.as_ref()).await?;

        data = match Self::decrypt_data(&data) {
            Ok(v) => v,
            Err(err) => {
                warn!("config decrypt failed, keeping file: {}, path: {}", err, path.as_ref());
                // keep the config file when decrypt failed - do not delete
                return Err(Error::ConfigNotFound);
            }
        };

        Ok(serde_json::from_slice(&data)?)
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
    #[tracing::instrument(level = "debug", skip(self, item, path))]
    async fn save_iam_config<Item: Serialize + Send>(&self, item: Item, path: impl AsRef<str> + Send) -> Result<()> {
        let mut data = serde_json::to_vec(&item)?;
        data = Self::encrypt_data(&data)?;

        let mut attempts = 0;
        let max_attempts = 5;
        let path_ref = path.as_ref();

        loop {
            match save_config(self.object_api.clone(), path_ref, data.clone()).await {
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
                    error!("Final failure saving IAM config to {}: {:?}", path_ref, e);
                    return Err(e.into());
                }
            }
        }
    }
    async fn delete_iam_config(&self, path: impl AsRef<str> + Send) -> Result<()> {
        delete_config(self.object_api.clone(), path.as_ref()).await?;
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
            error!("ObjectStore save failure for {}: {:?}", name, e);
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
                    warn!("load_user_identity failed: no such user, name: {name}, user_type: {user_type:?}");
                    Error::NoSuchUser(name.to_owned())
                } else {
                    warn!("load_user_identity failed: {err:?}, name: {name}, user_type: {user_type:?}");
                    err
                }
            })?;

        if u.credentials.is_expired() {
            let _ = self.delete_iam_config(get_user_identity_path(name, user_type)).await;
            let _ = self.delete_iam_config(get_mapped_policy_path(name, user_type, false)).await;
            warn!(
                "load_user_identity failed: user is expired, delete the user and mapped policy, name: {name}, user_type: {user_type:?}"
            );
            return Err(Error::NoSuchUser(name.to_owned()));
        }

        if u.credentials.access_key.is_empty() {
            u.credentials.access_key = name.to_owned();
        }

        if !u.credentials.session_token.is_empty() {
            match extract_jwt_claims(&u) {
                Ok(claims) => {
                    u.credentials.claims = Some(claims);
                }
                Err(err) => {
                    if u.credentials.is_temp() {
                        let _ = self.delete_iam_config(get_user_identity_path(name, user_type)).await;
                        let _ = self.delete_iam_config(get_mapped_policy_path(name, user_type, false)).await;
                    }
                    warn!("extract_jwt_claims failed: {err:?}, name: {name}, user_type: {user_type:?}");
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
                warn!("list_iam_config_items {:?}", err);
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
                Error::NoSuchPolicy
            } else {
                err
            }
        })?;
        Ok(())
    }
    async fn load_group(&self, name: &str, m: &mut HashMap<String, GroupInfo>) -> Result<()> {
        let u: GroupInfo = self.load_iam_config(get_group_info_path(name)).await.map_err(|err| {
            if is_err_config_not_found(&err) {
                Error::NoSuchPolicy
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
                warn!("list_iam_config_items {:?}", err);
                let _ = ctx.cancel();

                return Err(err);
            }

            if let Some(item) = v.item {
                let name = rustfs_utils::path::dir(&item);
                if let Err(err) = self.load_group(&name, m).await {
                    if !is_err_no_such_group(&err) {
                        return Err(err);
                    }
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
                warn!("list_iam_config_items {:?}", err);
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
                warn!("list_iam_config_items {:?}", err);
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

                        info!("load policy: {}", policy_name);

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
                    info!("load policy: {}", policy_name);
                    policy_docs_cache.insert(policy_name, p);
                }

                policies_list = policies_list.split_off(32);
            }
        }

        cache.policy_docs.store(Arc::new(policy_docs_cache.update_load_time()));

        let mut user_items_cache = CacheEntity::default();

        // users
        if let Some(item_name_list) = listed_config_items.get(USERS_LIST_KEY) {
            let mut item_name_list = item_name_list.clone();

            // let mut items_cache = CacheEntity::default();

            loop {
                if item_name_list.len() < 32 {
                    let items = self.load_user_concurrent(&item_name_list, UserType::Reg).await?;

                    for (idx, p) in items.into_iter().enumerate() {
                        if p.credentials.access_key.is_empty() {
                            continue;
                        }

                        let name = rustfs_utils::path::dir(&item_name_list[idx]);
                        info!("load reg user: {}", name);
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
                    info!("load reg user: {}", name);
                    user_items_cache.insert(name, p);
                }

                item_name_list = item_name_list.split_off(32);
            }

            // cache.users.store(Arc::new(items_cache.update_load_time()));
        }

        // groups
        if let Some(item_name_list) = listed_config_items.get(GROUPS_LIST_KEY) {
            let mut items_cache = CacheEntity::default();

            for item in item_name_list.iter() {
                let name = rustfs_utils::path::dir(item);
                info!("load group: {}", name);
                if let Err(err) = self.load_group(&name, &mut items_cache).await {
                    return Err(Error::other(format!("load group failed: {err}")));
                };
            }

            cache.groups.store(Arc::new(items_cache.update_load_time()));
        }

        // user policies
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
                        info!("load user policy: {}", name);
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
                    info!("load user policy: {}", name);
                    items_cache.insert(name, p);
                }

                item_name_list = item_name_list.split_off(32);
            }

            cache.user_policies.store(Arc::new(items_cache.update_load_time()));
        }

        // group policy
        if let Some(item_name_list) = listed_config_items.get(POLICY_DB_GROUPS_LIST_KEY) {
            let mut items_cache = CacheEntity::default();

            for item in item_name_list.iter() {
                let name = item.trim_end_matches(".json");

                info!("load group policy: {}", name);
                if let Err(err) = self.load_mapped_policy(name, UserType::Reg, true, &mut items_cache).await {
                    if !is_err_no_such_policy(&err) {
                        return Err(Error::other(format!("load group policy failed: {err}")));
                    }
                };
            }

            cache.group_policies.store(Arc::new(items_cache.update_load_time()));
        }

        let mut sts_policies_cache = CacheEntity::default();

        // svc users
        if let Some(item_name_list) = listed_config_items.get(SVC_ACC_LIST_KEY) {
            let mut items_cache = HashMap::default();

            for item in item_name_list.iter() {
                let name = rustfs_utils::path::dir(item);
                info!("load svc user: {}", name);
                if let Err(err) = self.load_user(&name, UserType::Svc, &mut items_cache).await {
                    if !is_err_no_such_user(&err) {
                        return Err(Error::other(format!("load svc user failed: {err}")));
                    }
                };
            }

            for (_, v) in items_cache.iter() {
                let parent = v.credentials.parent_user.clone();
                if !user_items_cache.contains_key(&parent) {
                    info!("load sts user policy: {}", parent);
                    if let Err(err) = self
                        .load_mapped_policy(&parent, UserType::Sts, false, &mut sts_policies_cache)
                        .await
                    {
                        if !is_err_no_such_policy(&err) {
                            return Err(Error::other(format!("load_mapped_policy failed: {err}")));
                        }
                    }
                }
            }

            // Merge items_cache to user_items_cache
            user_items_cache.extend(items_cache);

            // cache.users.store(Arc::new(items_cache.update_load_time()));
        }

        cache.build_user_group_memberships();
        let mut sts_items_cache = CacheEntity::default();
        // sts users
        if let Some(item_name_list) = listed_config_items.get(STS_LIST_KEY) {
            for item in item_name_list.iter() {
                info!("load sts user path: {}", item);

                let name = rustfs_utils::path::dir(item);
                info!("load sts user: {}", name);
                if let Err(err) = self.load_user(&name, UserType::Sts, &mut sts_items_cache).await {
                    info!("load sts user failed: {}", err);
                };
            }
        }

        // sts user policy
        if let Some(item_name_list) = listed_config_items.get(POLICY_DB_STS_USERS_LIST_KEY) {
            for item in item_name_list.iter() {
                let name = item.trim_end_matches(".json");
                info!("load sts user policy: {}", name);
                if let Err(err) = self
                    .load_mapped_policy(name, UserType::Sts, false, &mut sts_policies_cache)
                    .await
                {
                    info!("load sts user policy failed: {}", err);
                };
            }
        }

        cache.users.store(Arc::new(user_items_cache.update_load_time()));
        cache.sts_accounts.store(Arc::new(sts_items_cache.update_load_time()));
        cache.sts_policies.store(Arc::new(sts_policies_cache.update_load_time()));

        Ok(())
    }

    // /// load all and make a new cache.
    // async fn load_all(&self, cache: &Cache) -> Result<()> {
    //     let _items = &[
    //         "policydb/",
    //         "policies/",
    //         "groups/",
    //         "policydb/users/",
    //         "policydb/groups/",
    //         "service-accounts/",
    //         "policydb/sts-users/",
    //         "sts/",
    //     ];

    //     let items = self.list_iam_config_items("config/iam/").await?;
    //     debug!("all iam items: {items:?}");

    //     let (policy_docs, users, user_policies, sts_policies, sts_accounts) = (
    //         Arc::new(tokio::sync::Mutex::new(CacheEntity::new(Self::get_default_policyes()))),
    //         Arc::new(tokio::sync::Mutex::new(CacheEntity::default())),
    //         Arc::new(tokio::sync::Mutex::new(CacheEntity::default())),
    //         Arc::new(tokio::sync::Mutex::new(CacheEntity::default())),
    //         Arc::new(tokio::sync::Mutex::new(CacheEntity::default())),
    //     );

    //     // Read 32 elements at a time
    //     let iter = items
    //         .iter()
    //         .map(|item| item.trim_start_matches("config/iam/"))
    //         .map(|item| split_path(item, item.starts_with("policydb/")))
    //         .filter_map(|(list_key, trimmed_item)| {
    //             debug!("list_key: {list_key}, trimmed_item: {trimmed_item}");

    //             if list_key == "format.json" {
    //                 return None;
    //             }

    //             let (policy_docs, users, user_policies, sts_policies, sts_accounts) = (
    //                 policy_docs.clone(),
    //                 users.clone(),
    //                 user_policies.clone(),
    //                 sts_policies.clone(),
    //                 sts_accounts.clone(),
    //             );

    //             Some(async move {
    //                 match list_key {
    //                     "policies/" => {
    //                         let trimmed_item = dir(trimmed_item);
    //                         let name = trimmed_item.trim_end_matches('/');
    //                         let policy_doc = self.load_policy(name).await?;
    //                         policy_docs.lock().await.insert(name.to_owned(), policy_doc);
    //                     }
    //                     "users/" => {
    //                         let name = dir(trimmed_item);
    //                         if let Some(user) = self.load_user_identity(UserType::Reg, &name).await? {
    //                             users.lock().await.insert(name.to_owned(), user);
    //                         };
    //                     }
    //                     "groups/" => {}
    //                     "policydb/users/" | "policydb/groups/" => {
    //                         let name = trimmed_item.strip_suffix(".json").unwrap_or(trimmed_item);
    //                         let mapped_policy = self
    //                             .load_mapped_policy(UserType::Reg, name, list_key == "policydb/groups/")
    //                             .await?;
    //                         if !mapped_policy.policies.is_empty() {
    //                             user_policies.lock().await.insert(name.to_owned(), mapped_policy);
    //                         }
    //                     }
    //                     "service-accounts/" => {
    //                         let trimmed_item = dir(trimmed_item);
    //                         let name = trimmed_item.trim_end_matches('/');
    //                         let Some(user) = self.load_user_identity(UserType::Svc, name).await? else {
    //                             return Ok(());
    //                         };

    //                         let parent = user.credentials.parent_user.clone();

    //                         {
    //                             users.lock().await.insert(name.to_owned(), user);
    //                         }

    //                         if users.lock().await.get(&parent).is_some() {
    //                             return Ok(());
    //                         }

    //                         match self.load_mapped_policy(UserType::Sts, parent.as_str(), false).await {
    //                             Ok(m) => sts_policies.lock().await.insert(name.to_owned(), m),
    //                             Err(Error::EcstoreError(e)) if is_err_config_not_found(&e) => return Ok(()),
    //                             Err(e) => return Err(e),
    //                         };
    //                     }
    //                     "sts/" => {
    //                         let name = dir(trimmed_item);
    //                         if let Some(user) = self.load_user_identity(UserType::Sts, &name).await? {
    //                             warn!("sts_accounts insert {}, user {:?}", name, &user.credentials.access_key);
    //                             sts_accounts.lock().await.insert(name.to_owned(), user);
    //                         };
    //                     }
    //                     "policydb/sts-users/" => {
    //                         let name = trimmed_item.strip_suffix(".json").unwrap_or(trimmed_item);
    //                         let mapped_policy = self.load_mapped_policy(UserType::Sts, name, false).await?;
    //                         if !mapped_policy.policies.is_empty() {
    //                             sts_policies.lock().await.insert(name.to_owned(), mapped_policy);
    //                         }
    //                     }
    //                     _ => {}
    //                 }

    //                 Result::Ok(())
    //             })
    //         });

    //     let mut all_futures = Vec::with_capacity(32);

    //     for f in iter {
    //         all_futures.push(f);

    //         if all_futures.len() == 32 {
    //             try_join_all(all_futures).await?;
    //             all_futures = Vec::with_capacity(32);
    //         }
    //     }

    //     if !all_futures.is_empty() {
    //         try_join_all(all_futures).await?;
    //     }

    //     if let Some(x) = Arc::into_inner(users) {
    //         cache.users.store(Arc::new(x.into_inner().update_load_time()))
    //     }

    //     if let Some(x) = Arc::into_inner(policy_docs) {
    //         cache.policy_docs.store(Arc::new(x.into_inner().update_load_time()))
    //     }
    //     if let Some(x) = Arc::into_inner(user_policies) {
    //         cache.user_policies.store(Arc::new(x.into_inner().update_load_time()))
    //     }
    //     if let Some(x) = Arc::into_inner(sts_policies) {
    //         cache.sts_policies.store(Arc::new(x.into_inner().update_load_time()))
    //     }
    //     if let Some(x) = Arc::into_inner(sts_accounts) {
    //         cache.sts_accounts.store(Arc::new(x.into_inner().update_load_time()))
    //     }

    //     Ok(())
    // }
}
