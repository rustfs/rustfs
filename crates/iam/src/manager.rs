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

use crate::error::{Error, Result, is_err_config_not_found};
use crate::sys::get_claims_from_token_with_secret;
use crate::{
    cache::{Cache, CacheEntity},
    error::{Error as IamError, is_err_no_such_group, is_err_no_such_policy, is_err_no_such_user},
    store::{GroupInfo, MappedPolicy, Store, UserType, object::IAM_CONFIG_PREFIX},
    sys::{
        MAX_SVCSESSION_POLICY_SIZE, SESSION_POLICY_NAME, SESSION_POLICY_NAME_EXTRACTED, STATUS_DISABLED, STATUS_ENABLED,
        UpdateServiceAccountOpts,
    },
};
use futures::future::join_all;
use rustfs_credentials::{Credentials, EMBEDDED_POLICY_TYPE, INHERITED_POLICY_TYPE, get_global_action_cred};
use rustfs_madmin::{AccountStatus, AddOrUpdateUserReq, GroupDesc};
use rustfs_policy::{
    arn::ARN,
    auth::{self, UserIdentity, is_secret_key_valid, jwt_sign},
    format::Format,
    policy::{Policy, PolicyDoc, default::DEFAULT_POLICIES, iam_policy_claim_name_sa},
};
use rustfs_utils::path::path_join_buf;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::atomic::AtomicU8;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI64, Ordering},
    },
    time::Duration,
};
use time::OffsetDateTime;
use tokio::{
    select,
    sync::{
        mpsc,
        mpsc::{Receiver, Sender},
    },
};
use tracing::warn;
use tracing::{error, info};

const IAM_FORMAT_FILE: &str = "format.json";
const IAM_FORMAT_VERSION_1: i32 = 1;

#[derive(Serialize, Deserialize)]
struct IAMFormat {
    version: i32,
}

impl IAMFormat {
    fn new_version_1() -> Self {
        IAMFormat {
            version: IAM_FORMAT_VERSION_1,
        }
    }
}

fn get_iam_format_file_path() -> String {
    path_join_buf(&[&IAM_CONFIG_PREFIX, IAM_FORMAT_FILE])
}

#[repr(u8)]
#[derive(Debug, PartialEq)]
pub enum IamState {
    Uninitialized = 0,
    Loading = 1,
    Ready = 2,
    Error = 3,
}

pub struct IamCache<T> {
    pub cache: Cache,
    pub api: T,
    pub state: Arc<AtomicU8>,
    pub loading: Arc<AtomicBool>,
    pub roles: HashMap<ARN, Vec<String>>,
    pub send_chan: Sender<i64>,
    pub last_timestamp: AtomicI64,
}

impl<T> IamCache<T>
where
    T: Store,
{
    /// Create a new IAM system instance
    /// # Arguments
    /// * `api` - The storage backend implementing the Store trait
    ///
    /// # Returns
    /// An Arc-wrapped instance of IamSystem
    pub(crate) async fn new(api: T) -> Arc<Self> {
        let (sender, receiver) = mpsc::channel::<i64>(100);

        let sys = Arc::new(Self {
            api,
            cache: Cache::default(),
            state: Arc::new(AtomicU8::new(IamState::Uninitialized as u8)),
            loading: Arc::new(AtomicBool::new(false)),
            send_chan: sender,
            roles: HashMap::new(),
            last_timestamp: AtomicI64::new(0),
        });

        sys.clone().init(receiver).await.unwrap();
        sys
    }

    /// Initialize the IAM system
    async fn init(self: Arc<Self>, receiver: Receiver<i64>) -> Result<()> {
        self.state.store(IamState::Loading as u8, Ordering::SeqCst);
        // Ensure the IAM format file is persisted first
        self.clone().save_iam_formatter().await?;

        // Critical: Load all existing users/policies into memory cache
        const MAX_RETRIES: usize = 3;
        for attempt in 0..MAX_RETRIES {
            if let Err(e) = self.clone().load().await {
                if attempt == MAX_RETRIES - 1 {
                    self.state.store(IamState::Error as u8, Ordering::SeqCst);
                    error!("IAM fail to load initial data after {} attempts: {:?}", MAX_RETRIES, e);
                    return Err(e);
                } else {
                    warn!("IAM load failed, retrying... attempt {}", attempt + 1);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            } else {
                break;
            }
        }
        self.state.store(IamState::Ready as u8, Ordering::SeqCst);
        info!("IAM System successfully initialized and marked as READY");

        // Background ticker for synchronization
        // Check if environment variable is set
        let skip_background_task = std::env::var("RUSTFS_SKIP_BACKGROUND_TASK").is_ok();

        if !skip_background_task {
            // Background thread starts periodic updates or receives signal updates
            tokio::spawn({
                let s = Arc::clone(&self);
                async move {
                    let ticker = tokio::time::interval(Duration::from_secs(120));
                    tokio::pin!(ticker, receiver);
                    loop {
                        select! {
                            _ = ticker.tick() => {
                                info!("iam load ticker");
                                if let Err(err) =s.clone().load().await{
                                    error!("iam load err {:?}", err);
                                }
                            },
                            i = receiver.recv() => {
                                info!("iam load receiver");
                                match i {
                                    Some(t) => {
                                        let last = s.last_timestamp.load(Ordering::Relaxed);
                                        if last <= t {
                                            info!("iam load receiver load");
                                            if let Err(err) =s.clone().load().await{
                                                error!("iam load err {:?}", err);
                                            }
                                            ticker.reset();
                                        }
                                    },
                                    None => return,
                                }
                            }
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// Check if IAM system is ready
    pub fn is_ready(&self) -> bool {
        self.state.load(Ordering::SeqCst) == IamState::Ready as u8
    }

    async fn _notify(&self) {
        self.send_chan.send(OffsetDateTime::now_utc().unix_timestamp()).await.unwrap();
    }

    async fn load(self: Arc<Self>) -> Result<()> {
        // debug!("load iam to cache");
        self.api.load_all(&self.cache).await?;
        self.last_timestamp
            .store(OffsetDateTime::now_utc().unix_timestamp(), Ordering::Relaxed);
        Ok(())
    }

    pub async fn load_user(&self, access_key: &str) -> Result<()> {
        let mut users_map: HashMap<String, UserIdentity> = HashMap::new();
        let mut user_policy_map = HashMap::new();
        let mut sts_users_map = HashMap::new();
        let mut sts_policy_map = HashMap::new();
        let mut policy_docs_map = HashMap::new();

        let _ = self.api.load_user(access_key, UserType::Svc, &mut users_map).await;

        let parent_user = users_map.get(access_key).map(|svc| svc.credentials.parent_user.clone());

        if let Some(parent_user) = parent_user {
            let _ = self.api.load_user(&parent_user, UserType::Reg, &mut users_map).await;
            let _ = self
                .api
                .load_mapped_policy(&parent_user, UserType::Reg, false, &mut user_policy_map)
                .await;
        } else {
            let _ = self.api.load_user(access_key, UserType::Reg, &mut users_map).await;
            if users_map.contains_key(access_key) {
                let _ = self
                    .api
                    .load_mapped_policy(access_key, UserType::Reg, false, &mut user_policy_map)
                    .await;
            }

            let _ = self.api.load_user(access_key, UserType::Sts, &mut sts_users_map).await;

            let has_sts_user = sts_users_map.get(access_key);

            let sts_parent = has_sts_user.map(|sts| sts.credentials.parent_user.clone());
            if let Some(parent) = sts_parent {
                let _ = self
                    .api
                    .load_mapped_policy(&parent, UserType::Sts, false, &mut sts_policy_map)
                    .await;
            }

            let sts_user = has_sts_user.map(|sts| sts.credentials.access_key.clone());
            if let Some(ref sts) = sts_user {
                if let Some(plc) = sts_policy_map.get(sts) {
                    for p in plc.to_slice().iter() {
                        if !policy_docs_map.contains_key(p) {
                            let _ = self.api.load_policy_doc(p, &mut policy_docs_map).await;
                        }
                    }
                }
            }
        }

        if let Some(plc) = user_policy_map.get(access_key) {
            for p in plc.to_slice().iter() {
                if !policy_docs_map.contains_key(p) {
                    let _ = self.api.load_policy_doc(p, &mut policy_docs_map).await;
                }
            }
        }

        if let Some(user) = users_map.get(access_key) {
            Cache::add_or_update(&self.cache.users, access_key, user, OffsetDateTime::now_utc());
        }
        if let Some(user_policy) = user_policy_map.get(access_key) {
            Cache::add_or_update(&self.cache.user_policies, access_key, user_policy, OffsetDateTime::now_utc());
        }
        if let Some(sts_user) = sts_users_map.get(access_key) {
            Cache::add_or_update(&self.cache.sts_accounts, access_key, sts_user, OffsetDateTime::now_utc());
        }
        if let Some(sts_policy) = sts_policy_map.get(access_key) {
            Cache::add_or_update(&self.cache.sts_policies, access_key, sts_policy, OffsetDateTime::now_utc());
        }
        if let Some(policy_doc) = policy_docs_map.get(access_key) {
            Cache::add_or_update(&self.cache.policy_docs, access_key, policy_doc, OffsetDateTime::now_utc());
        }

        Ok(())
    }

    // TODO: Check if exists, whether retry is possible
    #[tracing::instrument(level = "debug", skip(self))]
    async fn save_iam_formatter(self: Arc<Self>) -> Result<()> {
        let path = get_iam_format_file_path();

        let iam_fmt: Format = match self.api.load_iam_config(&path).await {
            Ok(v) => v,
            Err(err) => {
                if !is_err_config_not_found(&err) {
                    return Err(err);
                }

                Format::default()
            }
        };

        if iam_fmt.version >= IAM_FORMAT_VERSION_1 {
            return Ok(());
        }

        self.api.save_iam_config(IAMFormat::new_version_1(), path).await
    }
    pub async fn get_user(&self, access_key: &str) -> Option<UserIdentity> {
        self.cache
            .users
            .load()
            .get(access_key)
            .cloned()
            .or_else(|| self.cache.sts_accounts.load().get(access_key).cloned())
    }

    pub async fn get_mapped_policy(&self, name: &str, is_group: bool) -> Option<MappedPolicy> {
        if is_group {
            self.cache.group_policies.load().get(name).cloned()
        } else {
            self.cache.user_policies.load().get(name).cloned()
        }
    }

    pub async fn get_policy(&self, name: &str) -> Result<Policy> {
        if name.is_empty() {
            return Err(Error::InvalidArgument);
        }

        let policies = MappedPolicy::new(name).to_slice();

        let mut to_merge = Vec::new();
        for policy in policies {
            if policy.is_empty() {
                continue;
            }

            let v = self
                .cache
                .policy_docs
                .load()
                .get(&policy)
                .cloned()
                .ok_or(Error::NoSuchPolicy)?;

            to_merge.push(v.policy);
        }

        if to_merge.is_empty() {
            return Err(Error::NoSuchPolicy);
        }

        Ok(Policy::merge_policies(to_merge))
    }

    pub async fn get_policy_doc(&self, name: &str) -> Result<PolicyDoc> {
        if name.is_empty() {
            return Err(Error::InvalidArgument);
        }

        self.cache.policy_docs.load().get(name).cloned().ok_or(Error::NoSuchPolicy)
    }

    pub async fn delete_policy(&self, name: &str, is_from_notify: bool) -> Result<()> {
        if name.is_empty() {
            return Err(Error::InvalidArgument);
        }

        if is_from_notify {
            let user_policy_cache = self.cache.user_policies.load();
            let group_policy_cache = self.cache.group_policies.load();
            let users_cache = self.cache.users.load();

            let mut users = Vec::new();
            user_policy_cache.iter().for_each(|(k, v)| {
                if !users_cache.contains_key(k) {
                    Cache::delete(&self.cache.user_policies, k, OffsetDateTime::now_utc());
                    return;
                }

                if v.policy_set().contains(name) {
                    users.push(k.to_owned());
                }
            });

            let mut groups = Vec::new();
            group_policy_cache.iter().for_each(|(k, v)| {
                if v.policy_set().contains(name) {
                    groups.push(k.to_owned());
                }
            });

            if !users.is_empty() || !groups.is_empty() {
                return Err(Error::PolicyInUse);
            }

            if let Err(err) = self.api.delete_policy_doc(name).await {
                if !is_err_no_such_policy(&err) {
                    Cache::delete(&self.cache.policy_docs, name, OffsetDateTime::now_utc());
                    return Ok(());
                }

                return Err(err);
            }
        }

        Cache::delete(&self.cache.policy_docs, name, OffsetDateTime::now_utc());

        Ok(())
    }

    pub async fn set_policy(&self, name: &str, policy: Policy) -> Result<OffsetDateTime> {
        if name.is_empty() || policy.is_empty() {
            return Err(Error::InvalidArgument);
        }

        let policy_doc = self
            .cache
            .policy_docs
            .load()
            .get(name)
            .map(|v| {
                let mut p = v.clone();
                p.update(policy.clone());
                p
            })
            .unwrap_or(PolicyDoc::new(policy));

        self.api.save_policy_doc(name, policy_doc.clone()).await?;

        let now = OffsetDateTime::now_utc();

        Cache::add_or_update(&self.cache.policy_docs, name, &policy_doc, now);

        Ok(now)
    }

    pub async fn list_polices(&self, bucket_name: &str) -> Result<HashMap<String, Policy>> {
        let mut m = HashMap::new();

        self.api.load_policy_docs(&mut m).await?;
        set_default_canned_policies(&mut m);

        let cache = CacheEntity::new(m.clone()).update_load_time();

        self.cache.policy_docs.store(Arc::new(cache));

        let items: Vec<_> = m.into_iter().map(|(k, v)| (k, v.policy.clone())).collect();

        let futures: Vec<_> = items.iter().map(|(_, policy)| policy.match_resource(bucket_name)).collect();

        let results = join_all(futures).await;

        let filtered = items
            .into_iter()
            .zip(results)
            .filter_map(|((k, policy), matches)| {
                if bucket_name.is_empty() || matches {
                    Some((k, policy))
                } else {
                    None
                }
            })
            .collect();

        Ok(filtered)
    }

    pub async fn merge_policies(&self, name: &str) -> (String, Policy) {
        let mut policies = Vec::new();
        let mut to_merge = Vec::new();
        let mut miss_policies = Vec::new();

        for policy in MappedPolicy::new(name).to_slice() {
            if policy.is_empty() {
                continue;
            }

            if let Some(v) = self.cache.policy_docs.load().get(&policy).cloned() {
                policies.push(policy);
                to_merge.push(v.policy);
            } else {
                miss_policies.push(policy);
            }
        }

        if !miss_policies.is_empty() {
            let mut m = HashMap::new();
            for policy in miss_policies {
                let _ = self.api.load_policy_doc(&policy, &mut m).await;
            }

            for (k, v) in m.iter() {
                Cache::add_or_update(&self.cache.policy_docs, k, v, OffsetDateTime::now_utc());

                policies.push(k.clone());
                to_merge.push(v.policy.clone());
            }
        }

        (policies.join(","), Policy::merge_policies(to_merge))
    }

    pub async fn list_policy_docs(&self, bucket_name: &str) -> Result<HashMap<String, PolicyDoc>> {
        let mut m = HashMap::new();

        self.api.load_policy_docs(&mut m).await?;
        set_default_canned_policies(&mut m);

        let cache = CacheEntity::new(m.clone()).update_load_time();

        self.cache.policy_docs.store(Arc::new(cache));

        let items: Vec<_> = m.into_iter().map(|(k, v)| (k, v.clone())).collect();

        let futures: Vec<_> = items
            .iter()
            .map(|(_, policy_doc)| policy_doc.policy.match_resource(bucket_name))
            .collect();

        let results = join_all(futures).await;

        let filtered = items
            .into_iter()
            .zip(results)
            .filter_map(|((k, policy_doc), matches)| {
                if bucket_name.is_empty() || matches {
                    Some((k, policy_doc))
                } else {
                    None
                }
            })
            .collect();

        Ok(filtered)
    }

    pub async fn list_policy_docs_internal(&self, bucket_name: &str) -> Result<HashMap<String, PolicyDoc>> {
        let cache = self.cache.policy_docs.load();
        let items: Vec<_> = cache.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

        let futures: Vec<_> = items
            .iter()
            .map(|(_, policy_doc)| policy_doc.policy.match_resource(bucket_name))
            .collect();

        let results = join_all(futures).await;

        let ret = items
            .into_iter()
            .zip(results)
            .filter_map(|((k, policy_doc), matches)| {
                if bucket_name.is_empty() || matches {
                    Some((k, policy_doc))
                } else {
                    None
                }
            })
            .collect();

        Ok(ret)
    }

    pub async fn list_temp_accounts(&self, access_key: &str) -> Result<Vec<UserIdentity>> {
        let users = self.cache.users.load();
        let mut user_exists = false;
        let mut ret = Vec::new();

        for (_, v) in users.iter() {
            let is_derived = v.credentials.is_service_account() || v.credentials.is_temp();

            if !is_derived && v.credentials.access_key.as_str() == access_key {
                user_exists = true;
            } else if is_derived && v.credentials.parent_user == access_key {
                user_exists = true;
                if v.credentials.is_temp() {
                    let mut u = v.clone();
                    u.credentials.secret_key = String::new();
                    u.credentials.session_token = String::new();

                    ret.push(u);
                }
            }
        }

        if !user_exists {
            return Err(Error::NoSuchUser(access_key.to_string()));
        }

        Ok(ret)
    }

    pub async fn list_sts_accounts(&self, access_key: &str) -> Result<Vec<Credentials>> {
        let users = self.cache.users.load();
        Ok(users
            .values()
            .filter_map(|x| {
                if !access_key.is_empty() && x.credentials.parent_user.as_str() == access_key && x.credentials.is_temp() {
                    let mut c = x.credentials.clone();
                    c.secret_key = String::new();
                    c.session_token = String::new();
                    return Some(c);
                }

                None
            })
            .collect())
    }

    pub async fn list_service_accounts(&self, access_key: &str) -> Result<Vec<Credentials>> {
        let users = self.cache.users.load();
        Ok(users
            .values()
            .filter_map(|x| {
                if !access_key.is_empty()
                    && x.credentials.parent_user.as_str() == access_key
                    && x.credentials.is_service_account()
                {
                    let mut c = x.credentials.clone();
                    c.secret_key = String::new();
                    c.session_token = String::new();
                    return Some(c);
                }

                None
            })
            .collect())
    }

    /// create a service account and update cache
    pub async fn add_service_account(&self, cred: Credentials) -> Result<OffsetDateTime> {
        if cred.access_key.is_empty() || cred.parent_user.is_empty() {
            return Err(Error::InvalidArgument);
        }

        let users = self.cache.users.load();
        if let Some(x) = users.get(&cred.access_key) {
            if x.credentials.is_service_account() {
                return Err(Error::IAMActionNotAllowed);
            }
        }

        let u = UserIdentity::new(cred);

        self.api
            .save_user_identity(&u.credentials.access_key, UserType::Svc, u.clone(), None)
            .await?;

        self.update_user_with_claims(&u.credentials.access_key, u.clone())?;

        Ok(OffsetDateTime::now_utc())
    }

    pub async fn update_service_account(&self, name: &str, opts: UpdateServiceAccountOpts) -> Result<OffsetDateTime> {
        let Some(ui) = self.cache.users.load().get(name).cloned() else {
            return Err(Error::NoSuchServiceAccount(name.to_string()));
        };

        if !ui.credentials.is_service_account() {
            return Err(Error::NoSuchServiceAccount(name.to_string()));
        }

        let mut cr = ui.credentials.clone();
        let current_secret_key = cr.secret_key.clone();

        if let Some(secret) = opts.secret_key {
            if !is_secret_key_valid(&secret) {
                return Err(Error::InvalidSecretKeyLength);
            }
            cr.secret_key = secret;
        }

        if opts.name.is_some() {
            cr.name = opts.name;
        }

        if opts.description.is_some() {
            cr.description = opts.description;
        }

        if opts.expiration.is_some() {
            // TODO: check expiration
            cr.expiration = opts.expiration;
        }

        if let Some(status) = opts.status {
            match status.as_str() {
                val if val == AccountStatus::Enabled.as_ref() => cr.status = auth::ACCOUNT_ON.to_owned(),
                val if val == AccountStatus::Disabled.as_ref() => cr.status = auth::ACCOUNT_OFF.to_owned(),
                auth::ACCOUNT_ON => cr.status = auth::ACCOUNT_ON.to_owned(),
                auth::ACCOUNT_OFF => cr.status = auth::ACCOUNT_OFF.to_owned(),
                _ => cr.status = auth::ACCOUNT_OFF.to_owned(),
            }
        }

        let mut m: HashMap<String, Value> = get_claims_from_token_with_secret(&cr.session_token, &current_secret_key)?;
        m.remove(SESSION_POLICY_NAME_EXTRACTED);

        let nosp = if let Some(policy) = &opts.session_policy {
            policy.version.is_empty() && policy.statements.is_empty()
        } else {
            false
        };

        if m.contains_key(SESSION_POLICY_NAME) && nosp {
            m.remove(SESSION_POLICY_NAME);
            m.insert(iam_policy_claim_name_sa(), Value::String(INHERITED_POLICY_TYPE.to_owned()));
        }

        if let Some(session_policy) = &opts.session_policy {
            session_policy.validate()?;
            if !session_policy.version.is_empty() && !session_policy.statements.is_empty() {
                let policy_buf = serde_json::to_vec(&session_policy)?;
                if policy_buf.len() > MAX_SVCSESSION_POLICY_SIZE {
                    return Err(Error::PolicyTooLarge);
                }

                m.insert(
                    SESSION_POLICY_NAME.to_owned(),
                    Value::String(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&policy_buf)),
                );
                m.insert(iam_policy_claim_name_sa(), Value::String(EMBEDDED_POLICY_TYPE.to_owned()));
            }
        }

        m.insert("accessKey".to_owned(), Value::String(name.to_owned()));

        cr.session_token = jwt_sign(&m, &cr.secret_key)?;

        let u = UserIdentity::new(cr);
        self.api
            .save_user_identity(&u.credentials.access_key, UserType::Svc, u.clone(), None)
            .await?;
        self.update_user_with_claims(&u.credentials.access_key, u.clone())?;

        Ok(OffsetDateTime::now_utc())
    }

    pub async fn policy_db_get(&self, name: &str, groups: &Option<Vec<String>>) -> Result<Vec<String>> {
        if name.is_empty() {
            return Err(Error::InvalidArgument);
        }

        let (mut policies, _) = self.policy_db_get_internal(name, false, false).await?;
        let present = !policies.is_empty();

        if let Some(groups) = groups {
            for group in groups.iter() {
                let (gp, _) = self.policy_db_get_internal(group, true, present).await?;
                gp.iter().for_each(|v| {
                    policies.push(v.clone());
                });
            }
        }

        Ok(policies)
    }

    async fn policy_db_get_internal(
        &self,
        name: &str,
        is_group: bool,
        policy_present: bool,
    ) -> Result<(Vec<String>, OffsetDateTime)> {
        if is_group {
            let groups = self.cache.groups.load();

            let g = match groups.get(name) {
                Some(p) => p.clone(),
                None => {
                    let mut m = HashMap::new();
                    self.api.load_group(name, &mut m).await?;
                    if let Some(p) = m.get(name) {
                        Cache::add_or_update(&self.cache.groups, name, p, OffsetDateTime::now_utc());
                    }

                    m.get(name).cloned().ok_or(Error::NoSuchGroup(name.to_string()))?
                }
            };

            if g.status == STATUS_DISABLED {
                return Ok((Vec::new(), OffsetDateTime::now_utc()));
            }

            if let Some(policy) = self.cache.group_policies.load().get(name) {
                return Ok((policy.to_slice(), policy.update_at));
            }

            if !policy_present {
                let mut m = HashMap::new();
                if let Err(err) = self.api.load_mapped_policy(name, UserType::Reg, true, &mut m).await {
                    if !is_err_no_such_policy(&err) {
                        return Err(err);
                    }
                }
                if let Some(p) = m.get(name) {
                    Cache::add_or_update(&self.cache.group_policies, name, p, OffsetDateTime::now_utc());
                    return Ok((p.to_slice(), p.update_at));
                }

                return Ok((Vec::new(), OffsetDateTime::now_utc()));
            }

            return Ok((Vec::new(), OffsetDateTime::now_utc()));
        }

        let users = self.cache.users.load();
        let u = users.get(name).cloned().unwrap_or_default();
        if !u.credentials.is_valid() {
            return Ok((Vec::new(), OffsetDateTime::now_utc()));
        }

        let mp = match self.cache.user_policies.load().get(name) {
            Some(p) => p.clone(),
            None => {
                let mut m = HashMap::new();
                if let Err(err) = self.api.load_mapped_policy(name, UserType::Reg, false, &mut m).await {
                    if !is_err_no_such_policy(&err) {
                        return Err(err);
                    }
                }
                if let Some(p) = m.get(name) {
                    Cache::add_or_update(&self.cache.user_policies, name, p, OffsetDateTime::now_utc());
                    p.clone()
                } else {
                    match self.cache.sts_policies.load().get(name) {
                        Some(p) => p.clone(),
                        None => {
                            let mut m = HashMap::new();
                            if let Err(err) = self.api.load_mapped_policy(name, UserType::Sts, false, &mut m).await {
                                if !is_err_no_such_policy(&err) {
                                    return Err(err);
                                }
                            }
                            if let Some(p) = m.get(name) {
                                Cache::add_or_update(&self.cache.sts_policies, name, p, OffsetDateTime::now_utc());
                                p.clone()
                            } else {
                                MappedPolicy::default()
                            }
                        }
                    }
                }
            }
        };

        let mut policies: HashSet<String> = mp.to_slice().into_iter().collect();

        if let Some(groups) = u.credentials.groups.as_ref() {
            for group in groups.iter() {
                if self
                    .cache
                    .groups
                    .load()
                    .get(group)
                    .filter(|v| v.status == STATUS_DISABLED)
                    .is_some()
                {
                    return Ok((Vec::new(), OffsetDateTime::now_utc()));
                }

                let mp = match self.cache.group_policies.load().get(group) {
                    Some(p) => p.clone(),
                    None => {
                        let mut m = HashMap::new();
                        if let Err(err) = self.api.load_mapped_policy(group, UserType::Reg, true, &mut m).await {
                            if !is_err_no_such_policy(&err) {
                                return Err(err);
                            }
                        }
                        if let Some(p) = m.get(group) {
                            Cache::add_or_update(&self.cache.group_policies, group, p, OffsetDateTime::now_utc());
                            p.clone()
                        } else {
                            MappedPolicy::default()
                        }
                    }
                };

                mp.to_slice().iter().for_each(|v| {
                    policies.insert(v.clone());
                });
            }
        }

        let update_at = mp.update_at;

        for group in self
            .cache
            .user_group_memberships
            .load()
            .get(name)
            .cloned()
            .unwrap_or_default()
            .iter()
        {
            if self
                .cache
                .groups
                .load()
                .get(group)
                .filter(|v| v.status == STATUS_DISABLED)
                .is_some()
            {
                return Ok((Vec::new(), OffsetDateTime::now_utc()));
            }

            let mp = match self.cache.group_policies.load().get(group) {
                Some(p) => p.clone(),
                None => {
                    let mut m = HashMap::new();
                    if let Err(err) = self.api.load_mapped_policy(group, UserType::Reg, true, &mut m).await {
                        if !is_err_no_such_policy(&err) {
                            return Err(err);
                        }
                    }
                    if let Some(p) = m.get(group) {
                        Cache::add_or_update(&self.cache.group_policies, group, p, OffsetDateTime::now_utc());
                        p.clone()
                    } else {
                        MappedPolicy::default()
                    }
                }
            };

            mp.to_slice().iter().for_each(|v| {
                policies.insert(v.clone());
            });
        }

        Ok((policies.into_iter().collect(), update_at))
    }
    pub async fn policy_db_set(&self, name: &str, user_type: UserType, is_group: bool, policy: &str) -> Result<OffsetDateTime> {
        if name.is_empty() {
            return Err(Error::InvalidArgument);
        }

        if policy.is_empty() {
            if let Err(err) = self.api.delete_mapped_policy(name, user_type, is_group).await {
                if !is_err_no_such_policy(&err) {
                    return Err(err);
                }
            }

            if is_group {
                Cache::delete(&self.cache.group_policies, name, OffsetDateTime::now_utc());
            } else if user_type == UserType::Sts {
                Cache::delete(&self.cache.sts_policies, name, OffsetDateTime::now_utc());
            } else {
                Cache::delete(&self.cache.user_policies, name, OffsetDateTime::now_utc());
            }

            return Ok(OffsetDateTime::now_utc());
        }

        let mp = MappedPolicy::new(policy);

        let policy_docs_cache = self.cache.policy_docs.load();
        for p in mp.to_slice() {
            if !policy_docs_cache.contains_key(&p) {
                return Err(Error::NoSuchPolicy);
            }
        }

        self.api
            .save_mapped_policy(name, user_type, is_group, mp.clone(), None)
            .await?;

        if is_group {
            Cache::add_or_update(&self.cache.group_policies, name, &mp, OffsetDateTime::now_utc());
        } else if user_type == UserType::Sts {
            Cache::add_or_update(&self.cache.sts_policies, name, &mp, OffsetDateTime::now_utc());
        } else {
            Cache::add_or_update(&self.cache.user_policies, name, &mp, OffsetDateTime::now_utc());
        }

        Ok(OffsetDateTime::now_utc())
    }

    pub async fn set_temp_user(&self, access_key: &str, cred: &Credentials, policy_name: Option<&str>) -> Result<OffsetDateTime> {
        if access_key.is_empty() || !cred.is_temp() || cred.is_expired() || cred.parent_user.is_empty() {
            error!(
                "set temp user invalid argument, access_key: {},  is_temp: {}, is_expired: {}, parent_user_empty: {}",
                access_key,
                cred.is_temp(),
                cred.is_expired(),
                cred.parent_user.is_empty()
            );
            return Err(Error::InvalidArgument);
        }

        if let Some(policy) = policy_name {
            let mp = MappedPolicy::new(policy);
            let (_, combined_policy_stmt) = filter_policies(&self.cache, &mp.policies, "temp");
            if combined_policy_stmt.is_empty() {
                return Err(Error::other(format!("Required policy not found: {}", IamError::NoSuchPolicy)));
            }

            self.api
                .save_mapped_policy(&cred.parent_user, UserType::Sts, false, mp.clone(), None)
                .await?;

            Cache::add_or_update(&self.cache.sts_policies, &cred.parent_user, &mp, OffsetDateTime::now_utc());
        }

        let u = UserIdentity::new(cred.clone());
        self.api
            .save_user_identity(access_key, UserType::Sts, u.clone(), None)
            .await?;

        Cache::add_or_update(&self.cache.sts_accounts, access_key, &u, OffsetDateTime::now_utc());

        Ok(OffsetDateTime::now_utc())
    }

    pub async fn get_user_info(&self, name: &str) -> Result<rustfs_madmin::UserInfo> {
        let users = self.cache.users.load();
        let policies = self.cache.user_policies.load();
        let group_members = self.cache.user_group_memberships.load();

        let u = match users.get(name) {
            Some(u) => u,
            None => return Err(Error::NoSuchUser(name.to_string())),
        };

        if u.credentials.is_temp() || u.credentials.is_service_account() {
            return Err(Error::IAMActionNotAllowed);
        }

        let mut uinfo = rustfs_madmin::UserInfo {
            status: if u.credentials.is_valid() {
                AccountStatus::Enabled
            } else {
                AccountStatus::Disabled
            },
            updated_at: u.update_at,
            ..Default::default()
        };

        if let Some(p) = policies.get(name) {
            uinfo.policy_name = Some(p.policies.clone());
            uinfo.updated_at = Some(p.update_at);
        }

        if let Some(members) = group_members.get(name) {
            uinfo.member_of = Some(members.iter().cloned().collect());
        }

        Ok(uinfo)
    }

    // returns all users (not STS or service accounts)
    pub async fn get_users(&self) -> Result<HashMap<String, rustfs_madmin::UserInfo>> {
        let mut m = HashMap::new();

        let users = self.cache.users.load();
        let policies = self.cache.user_policies.load();
        let group_members = self.cache.user_group_memberships.load();

        for (k, v) in users.iter() {
            if v.credentials.is_temp() || v.credentials.is_service_account() {
                continue;
            }

            let mut u = rustfs_madmin::UserInfo {
                status: if v.credentials.is_valid() {
                    AccountStatus::Enabled
                } else {
                    AccountStatus::Disabled
                },
                updated_at: v.update_at,
                ..Default::default()
            };

            if let Some(p) = policies.get(k) {
                u.policy_name = Some(p.policies.clone());
                u.updated_at = Some(p.update_at);
            }

            if let Some(members) = group_members.get(k) {
                u.member_of = Some(members.iter().cloned().collect());
            }

            m.insert(k.clone(), u);
        }

        Ok(m)
    }
    pub async fn get_bucket_users(&self, bucket_name: &str) -> Result<HashMap<String, rustfs_madmin::UserInfo>> {
        let users = self.cache.users.load();
        let policies_cache = self.cache.user_policies.load();
        let group_members = self.cache.user_group_memberships.load();
        let group_policy_cache = self.cache.group_policies.load();

        let mut ret = HashMap::new();

        for (k, v) in users.iter() {
            if v.credentials.is_temp() || v.credentials.is_service_account() {
                continue;
            }

            let mut policies = Vec::new();
            if let Some(p) = policies_cache.get(k) {
                policies.push(p.policies.clone());

                if let Some(groups) = group_members.get(k) {
                    for group in groups {
                        if let Some(p) = group_policy_cache.get(group) {
                            policies.push(p.policies.clone());
                        }
                    }
                }
            }

            let matched_policies = filter_policies(&self.cache, &policies.join(","), bucket_name).0;
            if matched_policies.is_empty() {
                continue;
            }

            let mut u = rustfs_madmin::UserInfo {
                policy_name: Some(matched_policies),
                status: if v.credentials.is_valid() {
                    AccountStatus::Enabled
                } else {
                    AccountStatus::Disabled
                },
                updated_at: v.update_at,
                ..Default::default()
            };

            if let Some(members) = group_members.get(k) {
                u.member_of = Some(members.iter().cloned().collect());
            }

            ret.insert(k.clone(), u);
        }

        Ok(ret)
    }
    pub async fn get_users_with_mapped_policies(&self) -> HashMap<String, String> {
        let mut m = HashMap::new();

        self.cache.user_policies.load().iter().for_each(|(k, v)| {
            m.insert(k.clone(), v.policies.clone());
        });

        self.cache.sts_policies.load().iter().for_each(|(k, v)| {
            m.insert(k.clone(), v.policies.clone());
        });

        m
    }

    pub async fn add_user(&self, access_key: &str, args: &AddOrUpdateUserReq) -> Result<OffsetDateTime> {
        let users = self.cache.users.load();
        if let Some(x) = users.get(access_key) {
            warn!("user already exists: {:?}", x);
            if x.credentials.is_temp() {
                return Err(Error::IAMActionNotAllowed);
            }
        }

        let status = {
            match &args.status {
                AccountStatus::Enabled => auth::ACCOUNT_ON,
                _ => auth::ACCOUNT_OFF,
            }
        };
        let user_entry = UserIdentity::from(Credentials {
            access_key: access_key.to_string(),
            secret_key: args.secret_key.to_string(),
            status: status.to_owned(),
            ..Default::default()
        });

        self.api
            .save_user_identity(access_key, UserType::Reg, user_entry.clone(), None)
            .await?;

        self.update_user_with_claims(access_key, user_entry)?;

        Ok(OffsetDateTime::now_utc())
    }

    pub async fn delete_user(&self, access_key: &str, utype: UserType) -> Result<()> {
        if access_key.is_empty() {
            return Err(Error::InvalidArgument);
        }

        if utype == UserType::Reg {
            if let Some(member_of) = self.cache.user_group_memberships.load().get(access_key) {
                for member in member_of.iter() {
                    let _ = self
                        .remove_members_from_group(member, vec![access_key.to_string()], false)
                        .await?;
                }
            }

            let users_cache = self.cache.users.load();

            for (_, v) in users_cache.iter() {
                let u = &v.credentials;
                if u.parent_user.as_str() == access_key {
                    if u.is_service_account() {
                        let _ = self.api.delete_user_identity(&u.access_key, UserType::Svc).await;
                        Cache::delete(&self.cache.users, &u.access_key, OffsetDateTime::now_utc());
                    }

                    if u.is_temp() {
                        let _ = self.api.delete_user_identity(&u.access_key, UserType::Sts).await;
                        Cache::delete(&self.cache.sts_accounts, &u.access_key, OffsetDateTime::now_utc());
                        Cache::delete(&self.cache.users, &u.access_key, OffsetDateTime::now_utc());
                    }
                }
            }
        }

        let _ = self.api.delete_mapped_policy(access_key, utype, false).await;

        Cache::delete(&self.cache.user_policies, access_key, OffsetDateTime::now_utc());

        if let Err(err) = self.api.delete_user_identity(access_key, utype).await {
            if !is_err_no_such_user(&err) {
                return Err(err);
            }
        }

        if utype == UserType::Sts {
            Cache::delete(&self.cache.sts_accounts, access_key, OffsetDateTime::now_utc());
        }

        Cache::delete(&self.cache.users, access_key, OffsetDateTime::now_utc());

        Ok(())
    }

    pub async fn update_user_secret_key(&self, access_key: &str, secret_key: &str) -> Result<()> {
        if access_key.is_empty() || secret_key.is_empty() {
            return Err(Error::InvalidArgument);
        }

        let users = self.cache.users.load();
        let u = match users.get(access_key) {
            Some(u) => u,
            None => return Err(Error::NoSuchUser(access_key.to_string())),
        };

        let mut cred = u.credentials.clone();
        cred.secret_key = secret_key.to_string();

        let u = UserIdentity::from(cred);

        self.api
            .save_user_identity(access_key, UserType::Reg, u.clone(), None)
            .await?;

        self.update_user_with_claims(access_key, u)
    }

    /// Add SSH public key for a user (for SFTP authentication)
    pub async fn add_user_ssh_public_key(&self, access_key: &str, public_key: &str) -> Result<()> {
        if access_key.is_empty() || public_key.is_empty() {
            return Err(Error::InvalidArgument);
        }

        let users = self.cache.users.load();
        let u = match users.get(access_key) {
            Some(u) => u,
            None => return Err(Error::NoSuchUser(access_key.to_string())),
        };

        let mut user_identity = u.clone();
        user_identity.add_ssh_public_key(public_key);

        self.api
            .save_user_identity(access_key, UserType::Reg, user_identity.clone(), None)
            .await?;

        self.update_user_with_claims(access_key, user_identity)
    }

    pub async fn set_user_status(&self, access_key: &str, status: AccountStatus) -> Result<OffsetDateTime> {
        if access_key.is_empty() {
            return Err(Error::InvalidArgument);
        }

        if !access_key.is_empty() && status != AccountStatus::Enabled && status != AccountStatus::Disabled {
            return Err(Error::InvalidArgument);
        }

        let users = self.cache.users.load();
        let u = match users.get(access_key) {
            Some(u) => u,
            None => return Err(Error::NoSuchUser(access_key.to_string())),
        };

        if u.credentials.is_temp() || u.credentials.is_service_account() {
            return Err(Error::IAMActionNotAllowed);
        }

        let status = {
            match status {
                AccountStatus::Enabled => auth::ACCOUNT_ON,
                _ => auth::ACCOUNT_OFF,
            }
        };

        let user_entry = UserIdentity::from(Credentials {
            access_key: access_key.to_string(),
            secret_key: u.credentials.secret_key.clone(),
            status: status.to_owned(),
            ..Default::default()
        });

        self.api
            .save_user_identity(access_key, UserType::Reg, user_entry.clone(), None)
            .await?;

        self.update_user_with_claims(access_key, user_entry)?;

        Ok(OffsetDateTime::now_utc())
    }

    fn update_user_with_claims(&self, k: &str, u: UserIdentity) -> Result<()> {
        let mut u = u;
        if !u.credentials.session_token.is_empty() {
            u.credentials.claims = Some(extract_jwt_claims(&u)?);
        }

        if u.credentials.is_temp() && !u.credentials.is_service_account() {
            Cache::add_or_update(&self.cache.sts_accounts, k, &u, OffsetDateTime::now_utc());
        } else {
            Cache::add_or_update(&self.cache.users, k, &u, OffsetDateTime::now_utc());
        }

        Ok(())
    }

    pub async fn is_temp_user(&self, access_key: &str) -> Result<(bool, String)> {
        let users = self.cache.users.load();
        let u = match users.get(access_key) {
            Some(u) => u,
            None => return Err(Error::NoSuchUser(access_key.to_string())),
        };

        if u.credentials.is_temp() {
            Ok((true, u.credentials.parent_user.clone()))
        } else {
            Ok((false, String::new()))
        }
    }

    pub async fn add_users_to_group(&self, group: &str, members: Vec<String>) -> Result<OffsetDateTime> {
        if group.is_empty() {
            return Err(Error::InvalidArgument);
        }

        let users_cache = self.cache.users.load();

        for member in members.iter() {
            if let Some(u) = users_cache.get(member) {
                if u.credentials.is_temp() || u.credentials.is_service_account() {
                    return Err(Error::IAMActionNotAllowed);
                }
            } else {
                return Err(Error::NoSuchUser(member.to_string()));
            }
        }

        let gi = match self.cache.groups.load().get(group) {
            Some(res) => {
                let mut gi = res.clone();
                let mut uniq_set: HashSet<String, std::collections::hash_map::RandomState> =
                    HashSet::from_iter(gi.members.iter().cloned());
                uniq_set.extend(members.iter().cloned());

                gi.members = uniq_set.into_iter().collect();
                gi
            }
            None => GroupInfo::new(members.clone()),
        };

        self.api.save_group_info(group, gi.clone()).await?;

        Cache::add_or_update(&self.cache.groups, group, &gi, OffsetDateTime::now_utc());

        let user_group_memberships = self.cache.user_group_memberships.load();
        members.iter().for_each(|member| {
            if let Some(m) = user_group_memberships.get(member) {
                let mut m = m.clone();
                m.insert(group.to_string());
                Cache::add_or_update(&self.cache.user_group_memberships, member, &m, OffsetDateTime::now_utc());
            }
        });

        Ok(OffsetDateTime::now_utc())
    }

    pub async fn set_group_status(&self, name: &str, enable: bool) -> Result<OffsetDateTime> {
        if name.is_empty() {
            return Err(Error::InvalidArgument);
        }

        let groups = self.cache.groups.load();
        let mut gi = match groups.get(name) {
            Some(gi) => gi.clone(),
            None => return Err(Error::NoSuchGroup(name.to_string())),
        };

        if enable {
            gi.status = STATUS_ENABLED.to_owned();
        } else {
            gi.status = STATUS_DISABLED.to_owned();
        }

        self.api.save_group_info(name, gi.clone()).await?;

        Cache::add_or_update(&self.cache.groups, name, &gi, OffsetDateTime::now_utc());

        Ok(OffsetDateTime::now_utc())
    }

    pub async fn get_group_description(&self, name: &str) -> Result<GroupDesc> {
        let (ps, updated_at) = self.policy_db_get_internal(name, true, false).await?;
        let policy = ps.join(",");

        let gi = self
            .cache
            .groups
            .load()
            .get(name)
            .cloned()
            .ok_or(Error::NoSuchGroup(name.to_string()))?;

        Ok(GroupDesc {
            name: name.to_string(),
            policy,
            members: gi.members,
            updated_at: Some(updated_at),
            status: gi.status,
        })
    }

    pub async fn list_groups(&self) -> Result<Vec<String>> {
        Ok(self.cache.groups.load().keys().cloned().collect())
    }

    pub async fn update_groups(&self) -> Result<Vec<String>> {
        let mut groups_set = HashSet::new();
        let mut m = HashMap::new();
        self.api.load_groups(&mut m).await?;
        for (group, gi) in m.iter() {
            Cache::add_or_update(&self.cache.groups, group, gi, OffsetDateTime::now_utc());
            groups_set.insert(group.to_string());
        }

        let mut m = HashMap::new();

        self.api.load_mapped_policies(UserType::Reg, true, &mut m).await?;
        for (group, gi) in m.iter() {
            Cache::add_or_update(&self.cache.group_policies, group, gi, OffsetDateTime::now_utc());
            groups_set.insert(group.to_string());
        }

        Ok(groups_set.into_iter().collect())
    }

    pub async fn remove_members_from_group(
        &self,
        name: &str,
        members: Vec<String>,
        update_cache_only: bool,
    ) -> Result<OffsetDateTime> {
        let mut gi = self
            .cache
            .groups
            .load()
            .get(name)
            .cloned()
            .ok_or(Error::NoSuchGroup(name.to_string()))?;

        let s: HashSet<&String> = HashSet::from_iter(gi.members.iter());
        let d: HashSet<&String> = HashSet::from_iter(members.iter());
        gi.members = s.difference(&d).map(|v| v.to_string()).collect::<Vec<String>>();

        if !update_cache_only {
            self.api.save_group_info(name, gi.clone()).await?;
        }

        Cache::add_or_update(&self.cache.groups, name, &gi, OffsetDateTime::now_utc());

        let user_group_memberships = self.cache.user_group_memberships.load();
        members.iter().for_each(|member| {
            if let Some(m) = user_group_memberships.get(member) {
                let mut m = m.clone();
                m.remove(name);
                Cache::add_or_update(&self.cache.user_group_memberships, member, &m, OffsetDateTime::now_utc());
            }
        });

        Ok(OffsetDateTime::now_utc())
    }

    pub async fn remove_users_from_group(&self, group: &str, members: Vec<String>) -> Result<OffsetDateTime> {
        if group.is_empty() {
            return Err(Error::InvalidArgument);
        }

        let users_cache = self.cache.users.load();

        for member in members.iter() {
            if let Some(u) = users_cache.get(member) {
                if u.credentials.is_temp() || u.credentials.is_service_account() {
                    return Err(Error::IAMActionNotAllowed);
                }
            } else {
                return Err(Error::NoSuchUser(member.to_string()));
            }
        }

        let gi = self
            .cache
            .groups
            .load()
            .get(group)
            .cloned()
            .ok_or(Error::NoSuchGroup(group.to_string()))?;

        if members.is_empty() && !gi.members.is_empty() {
            return Err(Error::GroupNotEmpty);
        }

        if members.is_empty() {
            if let Err(err) = self.api.delete_mapped_policy(group, UserType::Reg, true).await {
                if !is_err_no_such_policy(&err) {
                    return Err(err);
                }
            }

            if let Err(err) = self.api.delete_group_info(group).await {
                if !is_err_no_such_group(&err) {
                    return Err(err);
                }
            }

            Cache::delete(&self.cache.groups, group, OffsetDateTime::now_utc());
            Cache::delete(&self.cache.group_policies, group, OffsetDateTime::now_utc());

            return Ok(OffsetDateTime::now_utc());
        }

        self.remove_members_from_group(group, members, false).await
    }

    fn remove_group_from_memberships_map(&self, group: &str) {
        let user_group_memberships = self.cache.user_group_memberships.load();
        for (k, v) in user_group_memberships.iter() {
            if v.contains(group) {
                let mut m = v.clone();
                m.remove(group);
                Cache::add_or_update(&self.cache.user_group_memberships, k, &m, OffsetDateTime::now_utc());
            }
        }
    }

    fn update_group_memberships_map(&self, group: &str, gi: &GroupInfo) {
        let user_group_memberships = self.cache.user_group_memberships.load();
        for member in gi.members.iter() {
            if let Some(m) = user_group_memberships.get(member) {
                let mut m = m.clone();
                m.insert(group.to_string());
                Cache::add_or_update(&self.cache.user_group_memberships, member, &m, OffsetDateTime::now_utc());
            }
        }
    }

    pub async fn group_notification_handler(&self, group: &str) -> Result<()> {
        let mut m = HashMap::new();
        if let Err(err) = self.api.load_group(group, &mut m).await {
            if !is_err_no_such_group(&err) {
                return Err(err);
            }

            self.remove_group_from_memberships_map(group);
            Cache::delete(&self.cache.groups, group, OffsetDateTime::now_utc());
            Cache::delete(&self.cache.group_policies, group, OffsetDateTime::now_utc());

            return Ok(());
        }

        let gi = m[group].clone();

        Cache::add_or_update(&self.cache.groups, group, &gi, OffsetDateTime::now_utc());

        self.remove_group_from_memberships_map(group);
        self.update_group_memberships_map(group, &gi);

        Ok(())
    }

    pub async fn policy_notification_handler(&self, policy: &str) -> Result<()> {
        let mut m = HashMap::new();
        if let Err(err) = self.api.load_policy_doc(policy, &mut m).await {
            if !is_err_no_such_policy(&err) {
                return Err(err);
            }

            Cache::delete(&self.cache.policy_docs, policy, OffsetDateTime::now_utc());

            let user_policy_cache = self.cache.user_policies.load();
            let users_cache = self.cache.users.load();
            for (k, v) in user_policy_cache.iter() {
                let mut set = v.policy_set();
                if set.contains(policy) {
                    if !users_cache.contains_key(k) {
                        Cache::delete(&self.cache.user_policies, k, OffsetDateTime::now_utc());
                        continue;
                    }

                    set.remove(policy);

                    let mp = MappedPolicy::new(&set.iter().cloned().collect::<Vec<_>>().join(","));

                    Cache::add_or_update(&self.cache.user_policies, k, &mp, OffsetDateTime::now_utc());
                }
            }

            let group_policy_cache = self.cache.group_policies.load();
            for (k, v) in group_policy_cache.iter() {
                let mut set = v.policy_set();
                if set.contains(policy) {
                    set.remove(policy);

                    let mp = MappedPolicy::new(&set.iter().cloned().collect::<Vec<_>>().join(","));
                    Cache::add_or_update(&self.cache.group_policies, k, &mp, OffsetDateTime::now_utc());
                }
            }

            return Ok(());
        }

        Cache::add_or_update(&self.cache.policy_docs, policy, &m[policy], OffsetDateTime::now_utc());

        Ok(())
    }

    pub async fn policy_mapping_notification_handler(&self, name: &str, user_type: UserType, is_group: bool) -> Result<()> {
        let mut m = HashMap::new();
        if let Err(err) = self.api.load_mapped_policy(name, user_type, is_group, &mut m).await {
            if !is_err_no_such_policy(&err) {
                return Err(err);
            }

            if is_group {
                Cache::delete(&self.cache.group_policies, name, OffsetDateTime::now_utc());
            } else if user_type == UserType::Sts {
                Cache::delete(&self.cache.sts_policies, name, OffsetDateTime::now_utc());
            } else {
                Cache::delete(&self.cache.user_policies, name, OffsetDateTime::now_utc());
            }

            return Ok(());
        }

        let mp = m[name].clone();

        if is_group {
            Cache::add_or_update(&self.cache.group_policies, name, &mp, OffsetDateTime::now_utc());
        } else if user_type == UserType::Sts {
            Cache::delete(&self.cache.sts_policies, name, OffsetDateTime::now_utc());
        } else {
            Cache::add_or_update(&self.cache.user_policies, name, &mp, OffsetDateTime::now_utc());
        }

        Ok(())
    }
    pub async fn user_notification_handler(&self, name: &str, user_type: UserType) -> Result<()> {
        let mut m = HashMap::new();
        if let Err(err) = self.api.load_user(name, user_type, &mut m).await {
            if !is_err_no_such_user(&err) {
                return Err(err);
            }

            if user_type == UserType::Sts {
                Cache::delete(&self.cache.sts_accounts, name, OffsetDateTime::now_utc());
            } else {
                Cache::delete(&self.cache.users, name, OffsetDateTime::now_utc());
            }

            let member_of = self.cache.user_group_memberships.load();
            if let Some(m) = member_of.get(name) {
                for group in m.iter() {
                    if let Err(err) = self.remove_members_from_group(group, vec![name.to_string()], true).await {
                        if !is_err_no_such_group(&err) {
                            return Err(err);
                        }
                    }
                }
            }

            if user_type == UserType::Reg {
                let users_cache = self.cache.users.load();
                for (_, v) in users_cache.iter() {
                    let u = &v.credentials;
                    if u.parent_user.as_str() == name && u.is_service_account() {
                        let _ = self.api.delete_user_identity(&u.access_key, UserType::Svc).await;
                        Cache::delete(&self.cache.users, &u.access_key, OffsetDateTime::now_utc());
                    }
                }

                let sts_accounts = self.cache.sts_accounts.load();
                if let Some(u) = sts_accounts.get(name) {
                    let u = &u.credentials;
                    if u.parent_user.as_str() == name {
                        let _ = self.api.delete_user_identity(&u.access_key, UserType::Sts).await;
                        Cache::delete(&self.cache.sts_accounts, &u.access_key, OffsetDateTime::now_utc());
                    }
                }
            }

            Cache::delete(&self.cache.user_policies, name, OffsetDateTime::now_utc());

            return Ok(());
        }

        let u = m[name].clone();

        match user_type {
            UserType::Sts => {
                let name = u.credentials.parent_user;
                let mut m = HashMap::new();
                if let Err(err) = self.api.load_mapped_policy(&name, user_type, false, &mut m).await {
                    if !is_err_no_such_policy(&err) {
                        return Err(err);
                    }

                    return Ok(());
                }

                Cache::add_or_update(&self.cache.sts_policies, &name, &m[&name], OffsetDateTime::now_utc());
            }
            UserType::Reg => {
                let mut m = HashMap::new();
                if let Err(err) = self.api.load_mapped_policy(name, user_type, false, &mut m).await {
                    if !is_err_no_such_policy(&err) {
                        return Err(err);
                    }
                    return Ok(());
                }

                Cache::add_or_update(&self.cache.sts_policies, name, &m[name], OffsetDateTime::now_utc());
            }

            UserType::Svc => {
                let users_cache = self.cache.users.load();
                if let Some(u) = users_cache.get(&u.credentials.parent_user) {
                    let mut m = HashMap::new();
                    if let Err(err) = self
                        .api
                        .load_mapped_policy(&u.credentials.parent_user, UserType::Reg, false, &mut m)
                        .await
                    {
                        if !is_err_no_such_policy(&err) {
                            return Err(err);
                        }
                        return Ok(());
                    }

                    Cache::add_or_update(
                        &self.cache.user_policies,
                        &u.credentials.parent_user,
                        &m[&u.credentials.parent_user],
                        OffsetDateTime::now_utc(),
                    );
                } else {
                    let mut m = HashMap::new();
                    if let Err(err) = self
                        .api
                        .load_mapped_policy(&u.credentials.parent_user, UserType::Sts, false, &mut m)
                        .await
                    {
                        if !is_err_no_such_policy(&err) {
                            return Err(err);
                        }
                        return Ok(());
                    }

                    Cache::add_or_update(
                        &self.cache.sts_policies,
                        &u.credentials.parent_user,
                        &m[&u.credentials.parent_user],
                        OffsetDateTime::now_utc(),
                    );
                }
            }
            UserType::None => {}
        }

        Ok(())
    }
}

pub fn get_default_policyes() -> HashMap<String, PolicyDoc> {
    let default_policies = &DEFAULT_POLICIES;
    default_policies
        .iter()
        .map(|(n, p)| {
            (
                n.to_string(),
                PolicyDoc {
                    version: 1,
                    policy: p.clone(),
                    ..Default::default()
                },
            )
        })
        .collect()
}

fn set_default_canned_policies(policies: &mut HashMap<String, PolicyDoc>) {
    let default_policies = &DEFAULT_POLICIES;
    for (k, v) in default_policies.iter() {
        let name = k.to_string();
        policies.entry(name).or_insert_with(|| PolicyDoc::default_policy(v.clone()));
    }
}

pub fn get_token_signing_key() -> Option<String> {
    if let Some(s) = get_global_action_cred() {
        Some(s.secret_key.clone())
    } else {
        None
    }
}

pub fn extract_jwt_claims(u: &UserIdentity) -> Result<HashMap<String, Value>> {
    let Some(sys_key) = get_token_signing_key() else {
        return Err(Error::other("global active sk not init"));
    };

    let keys = vec![&sys_key, &u.credentials.secret_key];

    for key in keys {
        if let Ok(claims) = get_claims_from_token_with_secret(&u.credentials.session_token, key) {
            return Ok(claims);
        }
    }
    Err(Error::other("unable to extract claims"))
}

fn filter_policies(cache: &Cache, policy_name: &str, bucket_name: &str) -> (String, Policy) {
    let mp = MappedPolicy::new(policy_name).to_slice();

    let mut policies = Vec::new();
    let mut to_merge = Vec::new();
    for policy in mp {
        if policy.is_empty() {
            continue;
        }

        if let Some(p) = cache.policy_docs.load().get(&policy) {
            if bucket_name.is_empty() || pollster::block_on(p.policy.match_resource(bucket_name)) {
                policies.push(policy);
                to_merge.push(p.policy.clone());
            }
        }
    }

    (policies.join(","), Policy::merge_policies(to_merge))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_policy::policy::{Policy, PolicyDoc};
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_iam_format_new_version_1() {
        let format = IAMFormat::new_version_1();
        assert_eq!(format.version, IAM_FORMAT_VERSION_1);
        assert_eq!(format.version, 1);
    }

    #[test]
    fn test_get_iam_format_file_path() {
        let path = get_iam_format_file_path();
        assert!(path.contains(IAM_FORMAT_FILE));
        assert!(path.contains(&*IAM_CONFIG_PREFIX));
        assert_eq!(path, format!("{}/{}", *IAM_CONFIG_PREFIX, IAM_FORMAT_FILE));
    }

    #[test]
    fn test_get_default_policies() {
        let policies = get_default_policyes();

        // Should contain some default policies
        assert!(!policies.is_empty());

        // Check that all values are PolicyDoc
        for (name, policy_doc) in &policies {
            assert!(!name.is_empty());
            // PolicyDoc.version is i64, not String
            assert!(policy_doc.version >= 0);
        }
    }

    #[test]
    fn test_get_token_signing_key() {
        // This function returns the global action credential's secret key
        // In test environment, it might be None
        let key = get_token_signing_key();
        // Just verify it doesn't panic and returns an Option
        if let Some(k) = key {
            assert!(!k.is_empty());
        } // This is acceptable in test environment when None
    }

    #[test]
    fn test_extract_jwt_claims_basic() {
        let user_identity = UserIdentity {
            version: 1,
            credentials: Credentials {
                access_key: "test-access-key".to_string(),
                secret_key: "test-secret-key".to_string(),
                session_token: "invalid-token".to_string(), // Invalid token for testing error handling
                expiration: None,
                status: "enabled".to_string(),
                parent_user: "".to_string(),
                groups: None,
                claims: Some({
                    let mut claims = HashMap::new();
                    claims.insert("sub".to_string(), json!("test-user"));
                    claims.insert("aud".to_string(), json!("test-audience"));
                    claims
                }),
                name: None,
                description: None,
            },
            update_at: Some(OffsetDateTime::now_utc()),
        };

        let result = extract_jwt_claims(&user_identity);
        // In test environment without proper JWT setup, this should fail
        assert!(result.is_err());
    }

    #[test]
    fn test_extract_jwt_claims_no_claims() {
        let user_identity = UserIdentity {
            version: 1,
            credentials: Credentials {
                access_key: "test-access-key".to_string(),
                secret_key: "test-secret-key".to_string(),
                session_token: "".to_string(), // Empty token
                expiration: None,
                status: "enabled".to_string(),
                parent_user: "".to_string(),
                groups: None,
                claims: None,
                name: None,
                description: None,
            },
            update_at: Some(OffsetDateTime::now_utc()),
        };

        let result = extract_jwt_claims(&user_identity);
        // Should fail with empty session token
        assert!(result.is_err());
    }

    #[test]
    fn test_filter_policies_empty_bucket() {
        let cache = Cache::default();
        let policy_name = "test-policy";
        let bucket_name = "";

        let (name, policy) = filter_policies(&cache, policy_name, bucket_name);

        // When cache is empty, should return empty name and empty policy
        assert_eq!(name, "");
        assert!(policy.statements.is_empty());
    }

    #[test]
    fn test_filter_policies_with_bucket() {
        let cache = Cache::default();
        let policy_name = "test-policy";
        let bucket_name = "test-bucket";

        let (name, policy) = filter_policies(&cache, policy_name, bucket_name);

        // When cache is empty, should return empty name and empty policy regardless of bucket
        assert_eq!(name, "");
        assert!(policy.statements.is_empty());
    }

    #[test]
    fn test_constants() {
        // Test that constants are properly defined
        assert_eq!(IAM_FORMAT_FILE, "format.json");
        assert_eq!(IAM_FORMAT_VERSION_1, 1);
    }

    #[test]
    fn test_iam_format_serialization() {
        let format = IAMFormat::new_version_1();

        // Test serialization
        let serialized = serde_json::to_string(&format).unwrap();
        assert!(serialized.contains("\"version\":1"));

        // Test deserialization
        let deserialized: IAMFormat = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.version, format.version);
    }

    #[test]
    fn test_mapped_policy_operations() {
        let policy_name = "test-policy";
        let mapped_policy = MappedPolicy::new(policy_name);

        // Test that MappedPolicy can be created
        let policies = mapped_policy.to_slice();
        assert!(!policies.is_empty());
        assert!(policies.iter().any(|p| p.contains(policy_name)));
    }

    #[test]
    fn test_user_identity_structure() {
        let credentials = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: "".to_string(),
            expiration: None,
            status: "enabled".to_string(),
            parent_user: "parent-user".to_string(),
            groups: Some(vec!["group1".to_string(), "group2".to_string()]),
            claims: None,
            name: None,
            description: None,
        };

        let user_identity = UserIdentity {
            version: 1,
            credentials,
            update_at: Some(OffsetDateTime::now_utc()),
        };

        // Test basic structure
        assert_eq!(user_identity.version, 1);
        assert_eq!(user_identity.credentials.access_key, "AKIAIOSFODNN7EXAMPLE");
        assert_eq!(user_identity.credentials.secret_key, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
        assert_eq!(user_identity.credentials.status, "enabled");
        assert_eq!(user_identity.credentials.parent_user, "parent-user");
        assert_eq!(user_identity.credentials.groups, Some(vec!["group1".to_string(), "group2".to_string()]));
    }

    #[test]
    fn test_policy_structure() {
        let policy = Policy {
            id: Default::default(),
            version: "2012-10-17".to_string(),
            statements: vec![],
        };

        // Test basic structure
        assert_eq!(policy.version, "2012-10-17");
        assert_eq!(policy.statements.len(), 0);
        assert!(policy.is_empty());
    }

    #[test]
    fn test_policy_doc_structure() {
        let policy = Policy {
            id: Default::default(),
            version: "2012-10-17".to_string(),
            statements: vec![],
        };

        let policy_doc = PolicyDoc {
            version: 1,
            policy,
            create_date: Some(OffsetDateTime::now_utc()),
            update_date: Some(OffsetDateTime::now_utc()),
        };

        // Test basic structure
        assert_eq!(policy_doc.version, 1);
        assert_eq!(policy_doc.policy.version, "2012-10-17");
        assert!(policy_doc.policy.statements.is_empty());
    }

    #[test]
    fn test_group_info_basic() {
        // Test that GroupInfo can be created and used
        let group_info = GroupInfo {
            version: 1,
            status: STATUS_ENABLED.to_string(),
            members: vec!["user1".to_string(), "user2".to_string()],
            update_at: Some(OffsetDateTime::now_utc()),
        };

        assert_eq!(group_info.version, 1);
        assert_eq!(group_info.status, STATUS_ENABLED);
        assert_eq!(group_info.members.len(), 2);
        assert!(group_info.members.contains(&"user1".to_string()));
        assert!(group_info.members.contains(&"user2".to_string()));
    }

    #[test]
    fn test_update_service_account_opts() {
        let policy = Policy {
            id: Default::default(),
            version: "2012-10-17".to_string(),
            statements: vec![],
        };

        let opts = UpdateServiceAccountOpts {
            secret_key: Some("new-secret-key".to_string()),
            status: Some(STATUS_ENABLED.to_string()),
            name: Some("service-account-name".to_string()),
            description: Some("Updated service account".to_string()),
            expiration: None,
            session_policy: Some(policy.clone()),
        };

        assert_eq!(opts.secret_key, Some("new-secret-key".to_string()));
        assert_eq!(opts.status, Some(STATUS_ENABLED.to_string()));
        assert_eq!(opts.name, Some("service-account-name".to_string()));
        assert_eq!(opts.description, Some("Updated service account".to_string()));
        assert!(opts.session_policy.is_some());
        assert!(opts.expiration.is_none());
    }

    #[test]
    fn test_status_constants() {
        // Test that status constants are properly defined
        assert_eq!(STATUS_ENABLED, "enabled");
        assert_eq!(STATUS_DISABLED, "disabled");
    }

    #[test]
    fn test_session_policy_constants() {
        // Test session policy related constants - these are compile-time constants
        // so we just verify they exist and have expected values
        assert_eq!(SESSION_POLICY_NAME, "sessionPolicy");
        assert_eq!(SESSION_POLICY_NAME_EXTRACTED, "sessionPolicy-extracted");
        // MAX_SVCSESSION_POLICY_SIZE is a positive constant defined at compile time
        assert_eq!(MAX_SVCSESSION_POLICY_SIZE, 4096); // Verify the actual expected value
    }

    #[test]
    fn test_credentials_validation() {
        let credentials = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: "".to_string(),
            expiration: None,
            status: "on".to_string(),
            parent_user: "".to_string(),
            groups: None,
            claims: None,
            name: None,
            description: None,
        };

        // Test validation methods
        assert!(credentials.is_valid());
        assert!(!credentials.is_expired());
        assert!(!credentials.is_temp());
        assert!(!credentials.is_service_account());
    }

    #[test]
    fn test_credentials_with_session_token() {
        let credentials = Credentials {
            access_key: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: "session-token".to_string(),
            expiration: Some(OffsetDateTime::now_utc() + time::Duration::hours(1)),
            status: "on".to_string(),
            parent_user: "".to_string(),
            groups: None,
            claims: None,
            name: None,
            description: None,
        };

        // Test temp credentials
        assert!(credentials.is_valid());
        assert!(!credentials.is_expired());
        assert!(credentials.is_temp());
    }

    #[test]
    fn test_policy_merge() {
        let policy1 = Policy {
            id: Default::default(),
            version: "2012-10-17".to_string(),
            statements: vec![],
        };

        let policy2 = Policy {
            id: Default::default(),
            version: "2012-10-17".to_string(),
            statements: vec![],
        };

        let merged = Policy::merge_policies(vec![policy1, policy2]);
        assert_eq!(merged.version, "2012-10-17");
        assert!(merged.statements.is_empty());
        assert!(merged.is_empty());
    }
}
