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

use crate::error::Error as IamError;
use crate::error::is_err_no_such_account;
use crate::error::is_err_no_such_temp_account;
use crate::error::is_err_no_such_user;
use crate::error::{Error, Result};
use crate::manager::extract_jwt_claims;
use crate::manager::get_default_policyes;
use crate::manager::{IamCache, IamSyncMetricsSnapshot};
use crate::store::GroupInfo;
use crate::store::MappedPolicy;
use crate::store::Store;
use crate::store::UserType;
use crate::utils::{extract_claims, extract_claims_allow_missing_exp};
use rustfs_credentials::{Credentials, EMBEDDED_POLICY_TYPE, INHERITED_POLICY_TYPE, get_global_action_cred};
use rustfs_ecstore::notification_sys::get_global_notification_sys;
use rustfs_madmin::AddOrUpdateUserReq;
use rustfs_madmin::GroupDesc;
use rustfs_policy::arn::ARN;
use rustfs_policy::auth::{
    ACCOUNT_ON, UserIdentity, contains_reserved_chars, create_new_credentials_with_metadata, generate_credentials,
    is_access_key_valid, is_secret_key_valid,
};
use rustfs_policy::policy::Args;
use rustfs_policy::policy::opa;
use rustfs_policy::policy::{Policy, PolicyDoc, iam_policy_claim_name_sa, policy_needs_existing_object_tag_for_args};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use time::OffsetDateTime;
use tokio::sync::RwLock;
use tracing::{error, info, warn};

pub const MAX_SVCSESSION_POLICY_SIZE: usize = 4096;

pub const STATUS_ENABLED: &str = "enabled";
pub const STATUS_DISABLED: &str = "disabled";

pub const POLICYNAME: &str = "policy";
pub const SESSION_POLICY_NAME: &str = "sessionPolicy";
pub const SESSION_POLICY_NAME_EXTRACTED: &str = "sessionPolicy-extracted";

static POLICY_PLUGIN_CLIENT: OnceLock<Arc<RwLock<Option<rustfs_policy::policy::opa::AuthZPlugin>>>> = OnceLock::new();

fn get_policy_plugin_client() -> Arc<RwLock<Option<rustfs_policy::policy::opa::AuthZPlugin>>> {
    POLICY_PLUGIN_CLIENT.get_or_init(|| Arc::new(RwLock::new(None))).clone()
}

pub struct IamSys<T> {
    store: Arc<IamCache<T>>,
    roles_map: HashMap<ARN, String>,
}

#[derive(Clone)]
enum PreparedSessionPolicy {
    None,
    DenyAll,
    Policy(Policy),
}

#[derive(Clone, Copy)]
enum PreparedServicePolicyMode {
    Inherited,
    SessionBound,
}

#[derive(Clone)]
enum PreparedIamMode {
    Opa,
    Owner,
    Deny,
    Regular {
        combined_policy: Policy,
    },
    Sts {
        is_owner: bool,
        combined_policy: Policy,
        session_policy: PreparedSessionPolicy,
    },
    ServiceAccount {
        is_owner: bool,
        parent_user: String,
        combined_policy: Policy,
        mode: PreparedServicePolicyMode,
        session_policy: PreparedSessionPolicy,
    },
}

#[derive(Clone)]
pub struct PreparedIamAuth {
    pub needs_existing_object_tag: bool,
    mode: PreparedIamMode,
}

impl PreparedIamAuth {
    /// Evaluate whether the already-prepared IAM context needs ExistingObjectTag
    /// conditions for the provided request args.
    pub async fn needs_existing_object_tag_for_args(&self, args: &Args<'_>) -> bool {
        match &self.mode {
            PreparedIamMode::Opa | PreparedIamMode::Owner | PreparedIamMode::Deny => false,
            PreparedIamMode::Regular { combined_policy } => {
                policy_needs_existing_object_tag_for_args(combined_policy, args).await
            }
            PreparedIamMode::Sts {
                combined_policy,
                session_policy,
                ..
            } => {
                policy_needs_existing_object_tag_for_args(combined_policy, args).await
                    || prepared_session_policy_needs_existing_object_tag_for_args(session_policy, args).await
            }
            PreparedIamMode::ServiceAccount {
                combined_policy,
                mode,
                session_policy,
                ..
            } => {
                policy_needs_existing_object_tag_for_args(combined_policy, args).await
                    || matches!(mode, PreparedServicePolicyMode::SessionBound)
                        && prepared_session_policy_needs_existing_object_tag_for_args(session_policy, args).await
            }
        }
    }

    /// Returns the resolved identity policy prepared for the current auth mode.
    ///
    /// This is intended for read-only views (for example `/accountinfo`) so
    /// callers can reuse the same policy resolution path as authorization.
    pub fn combined_policy_for_view(&self) -> Option<&Policy> {
        match &self.mode {
            PreparedIamMode::Regular { combined_policy } => Some(combined_policy),
            PreparedIamMode::Sts { combined_policy, .. } => Some(combined_policy),
            PreparedIamMode::ServiceAccount { combined_policy, .. } => Some(combined_policy),
            PreparedIamMode::Opa | PreparedIamMode::Owner | PreparedIamMode::Deny => None,
        }
    }
}

impl<T: Store> IamSys<T> {
    /// Create a new IamSys instance with the given IamCache store
    ///
    /// # Arguments
    /// * `store` - An Arc to the IamCache instance
    ///
    /// # Returns
    /// A new instance of IamSys
    pub fn new(store: Arc<IamCache<T>>) -> Self {
        tokio::spawn(async move {
            match opa::lookup_config().await {
                Ok(conf) => {
                    if conf.enable() {
                        Self::set_policy_plugin_client(opa::AuthZPlugin::new(conf)).await;
                        info!("OPA plugin enabled");
                    }
                }
                Err(e) => {
                    error!("Error loading OPA configuration err:{}", e);
                }
            };
        });

        Self {
            store,
            roles_map: HashMap::new(),
        }
    }

    /// Check if the IamSys has a watcher configured
    ///
    /// # Returns
    /// `true` if a watcher is configured, `false` otherwise
    pub fn has_watcher(&self) -> bool {
        self.store.api.has_watcher()
    }

    pub fn sync_metrics_snapshot(&self) -> IamSyncMetricsSnapshot {
        self.store.sync_metrics_snapshot()
    }

    pub async fn set_policy_plugin_client(client: rustfs_policy::policy::opa::AuthZPlugin) {
        let policy_plugin_client = get_policy_plugin_client();
        let mut guard = policy_plugin_client.write().await;
        *guard = Some(client);
    }

    pub async fn get_policy_plugin_client() -> Option<rustfs_policy::policy::opa::AuthZPlugin> {
        let policy_plugin_client = get_policy_plugin_client();
        let guard = policy_plugin_client.read().await;
        guard.clone()
    }

    pub async fn load_group(&self, name: &str) -> Result<()> {
        self.store.group_notification_handler(name).await
    }

    pub async fn load_groups(&self, m: &mut HashMap<String, GroupInfo>) -> Result<()> {
        self.store.api.load_groups(m).await
    }

    pub async fn load_policy(&self, name: &str) -> Result<()> {
        self.store.policy_notification_handler(name).await
    }

    pub async fn load_policy_mapping(&self, name: &str, user_type: UserType, is_group: bool) -> Result<()> {
        self.store
            .policy_mapping_notification_handler(name, user_type, is_group)
            .await
    }

    pub async fn load_user(&self, name: &str, user_type: UserType) -> Result<()> {
        self.store.user_notification_handler(name, user_type).await
    }

    pub async fn load_users(&self, user_type: UserType, m: &mut HashMap<String, UserIdentity>) -> Result<()> {
        self.store.api.load_users(user_type, m).await?;
        Ok(())
    }

    pub async fn load_service_account(&self, name: &str) -> Result<()> {
        self.store.user_notification_handler(name, UserType::Svc).await
    }

    pub async fn delete_policy(&self, name: &str, notify: bool) -> Result<()> {
        for k in get_default_policyes().keys() {
            if k == name {
                return Err(Error::other("system policy can not be deleted"));
            }
        }

        self.store.delete_policy(name, notify).await?;

        if !notify || self.has_watcher() {
            return Ok(());
        }

        if let Some(notification_sys) = get_global_notification_sys() {
            let resp = notification_sys.delete_policy(name).await;
            for r in resp {
                if let Some(err) = r.err {
                    warn!("notify delete_policy failed: {}", err);
                }
            }
        }

        Ok(())
    }

    pub async fn info_policy(&self, name: &str) -> Result<rustfs_madmin::PolicyInfo> {
        let d = self.store.get_policy_doc(name).await?;
        let pdata = serde_json::to_value(&d.policy)?;

        Ok(rustfs_madmin::PolicyInfo {
            policy_name: name.to_string(),
            policy: pdata,
            create_date: d.create_date,
            update_date: d.update_date,
        })
    }

    pub async fn load_mapped_policies(
        &self,
        user_type: UserType,
        is_group: bool,
        m: &mut HashMap<String, MappedPolicy>,
    ) -> Result<()> {
        self.store.api.load_mapped_policies(user_type, is_group, m).await
    }

    pub async fn list_polices(&self, bucket_name: &str) -> Result<HashMap<String, Policy>> {
        self.store.list_polices(bucket_name).await
    }

    pub async fn list_policy_docs(&self, bucket_name: &str) -> Result<HashMap<String, PolicyDoc>> {
        self.store.list_policy_docs(bucket_name).await
    }

    pub async fn set_policy(&self, name: &str, policy: Policy) -> Result<OffsetDateTime> {
        let updated_at = self.store.set_policy(name, policy).await?;

        if !self.has_watcher()
            && let Some(notification_sys) = get_global_notification_sys()
        {
            let resp = notification_sys.load_policy(name).await;
            for r in resp {
                if let Some(err) = r.err {
                    warn!("notify load_policy failed: {}", err);
                }
            }
        }

        Ok(updated_at)
    }

    pub async fn get_role_policy(&self, arn_str: &str) -> Result<(ARN, String)> {
        let Some(arn) = ARN::parse(arn_str).ok() else {
            return Err(Error::other("Invalid ARN"));
        };

        let Some(policy) = self.roles_map.get(&arn) else {
            return Err(Error::other("No such role"));
        };

        Ok((arn, policy.clone()))
    }

    pub async fn delete_user(&self, name: &str, notify: bool) -> Result<()> {
        self.store.delete_user(name, UserType::Reg).await?;

        if notify
            && !self.has_watcher()
            && let Some(notification_sys) = get_global_notification_sys()
        {
            let resp = notification_sys.delete_user(name).await;
            for r in resp {
                if let Some(err) = r.err {
                    warn!("notify delete_user failed: {}", err);
                }
            }
        }

        Ok(())
    }

    async fn notify_for_user(&self, name: &str, is_temp: bool) {
        if self.has_watcher() {
            return;
        }

        // Fire-and-forget notification to peers - don't block auth operations
        // This is critical for cluster recovery: login should not wait for dead peers
        let name = name.to_string();
        tokio::spawn(async move {
            if let Some(notification_sys) = get_global_notification_sys() {
                let resp = notification_sys.load_user(&name, is_temp).await;
                for r in resp {
                    if let Some(err) = r.err {
                        warn!("notify load_user failed (non-blocking): {}", err);
                    }
                }
            }
        });
    }

    async fn notify_for_service_account(&self, name: &str) {
        if self.has_watcher() {
            return;
        }

        // Fire-and-forget notification to peers - don't block service account operations
        let name = name.to_string();
        tokio::spawn(async move {
            if let Some(notification_sys) = get_global_notification_sys() {
                let resp = notification_sys.load_service_account(&name).await;
                for r in resp {
                    if let Some(err) = r.err {
                        warn!("notify load_service_account failed (non-blocking): {}", err);
                    }
                }
            }
        });
    }

    pub async fn current_policies(&self, name: &str) -> String {
        self.store.merge_policies(name).await.0
    }

    pub async fn list_bucket_users(&self, bucket_name: &str) -> Result<HashMap<String, rustfs_madmin::UserInfo>> {
        self.store.get_bucket_users(bucket_name).await
    }

    pub async fn list_users(&self) -> Result<HashMap<String, rustfs_madmin::UserInfo>> {
        self.store.get_users().await
    }

    pub async fn set_temp_user(&self, name: &str, cred: &Credentials, policy_name: Option<&str>) -> Result<OffsetDateTime> {
        let updated_at = self.store.set_temp_user(name, cred, policy_name).await?;

        self.notify_for_user(&cred.access_key, true).await;

        Ok(updated_at)
    }

    pub async fn is_temp_user(&self, name: &str) -> Result<(bool, String)> {
        let Some(u) = self.store.get_user(name).await else {
            return Err(IamError::NoSuchUser(name.to_string()));
        };
        if u.credentials.is_temp() {
            Ok((true, u.credentials.parent_user))
        } else {
            Ok((false, "".to_string()))
        }
    }
    pub async fn is_service_account(&self, name: &str) -> Result<(bool, String)> {
        let Some(u) = self.store.get_user(name).await else {
            return Err(IamError::NoSuchUser(name.to_string()));
        };

        if u.credentials.is_service_account() {
            Ok((true, u.credentials.parent_user))
        } else {
            Ok((false, "".to_string()))
        }
    }

    pub async fn get_user_info(&self, name: &str) -> Result<rustfs_madmin::UserInfo> {
        self.store.get_user_info(name).await
    }

    pub async fn set_user_status(&self, name: &str, status: rustfs_madmin::AccountStatus) -> Result<OffsetDateTime> {
        let updated_at = self.store.set_user_status(name, status).await?;

        self.notify_for_user(name, false).await;

        Ok(updated_at)
    }

    pub async fn new_service_account(
        &self,
        parent_user: &str,
        groups: Option<Vec<String>>,
        opts: NewServiceAccountOpts,
    ) -> Result<(Credentials, OffsetDateTime)> {
        if parent_user.is_empty() {
            return Err(IamError::InvalidArgument);
        }
        if !opts.access_key.is_empty() && opts.secret_key.is_empty() {
            return Err(IamError::NoSecretKeyWithAccessKey);
        }

        if !opts.secret_key.is_empty() && opts.access_key.is_empty() {
            return Err(IamError::NoAccessKeyWithSecretKey);
        }

        if parent_user == opts.access_key {
            return Err(IamError::IAMActionNotAllowed);
        }

        // TODO: check allow_site_replicator_account

        let policy_buf = if let Some(policy) = opts.session_policy {
            policy.validate()?;
            let buf = serde_json::to_vec(&policy)?;
            if buf.len() > MAX_SVCSESSION_POLICY_SIZE {
                return Err(IamError::PolicyTooLarge);
            }

            buf
        } else {
            Vec::new()
        };

        let mut m: HashMap<String, Value> = HashMap::new();
        m.insert("parent".to_owned(), Value::String(parent_user.to_owned()));

        if !policy_buf.is_empty() {
            m.insert(
                SESSION_POLICY_NAME.to_owned(),
                Value::String(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&policy_buf)),
            );
            m.insert(iam_policy_claim_name_sa(), Value::String(EMBEDDED_POLICY_TYPE.to_owned()));
        } else {
            m.insert(iam_policy_claim_name_sa(), Value::String(INHERITED_POLICY_TYPE.to_owned()));
        }

        if let Some(claims) = opts.claims {
            for (k, v) in claims.iter() {
                if !m.contains_key(k) {
                    m.insert(k.to_owned(), v.to_owned());
                }
            }
        }

        if let Some(expiration) = opts.expiration {
            m.insert("exp".to_string(), Value::Number(serde_json::Number::from(expiration.unix_timestamp())));
        }

        let (access_key, secret_key) = if !opts.access_key.is_empty() || !opts.secret_key.is_empty() {
            (opts.access_key, opts.secret_key)
        } else {
            generate_credentials()?
        };

        let mut cred = create_new_credentials_with_metadata(&access_key, &secret_key, &m, &secret_key)?;
        cred.parent_user = parent_user.to_owned();
        cred.groups = groups;
        cred.status = ACCOUNT_ON.to_owned();
        cred.name = opts.name;
        cred.description = opts.description;

        let create_at = self.store.add_service_account(cred.clone()).await?;

        self.notify_for_service_account(&cred.access_key).await;

        Ok((cred, create_at))
    }

    pub async fn update_service_account(&self, name: &str, opts: UpdateServiceAccountOpts) -> Result<OffsetDateTime> {
        let updated_at = self.store.update_service_account(name, opts).await?;

        self.notify_for_service_account(name).await;

        Ok(updated_at)
    }

    pub async fn list_service_accounts(&self, access_key: &str) -> Result<Vec<Credentials>> {
        self.store.list_service_accounts(access_key).await
    }

    pub async fn list_temp_accounts(&self, access_key: &str) -> Result<Vec<UserIdentity>> {
        self.store.list_temp_accounts(access_key).await
    }

    pub async fn list_sts_accounts(&self, access_key: &str) -> Result<Vec<Credentials>> {
        self.store.list_sts_accounts(access_key).await
    }

    pub async fn get_service_account(&self, access_key: &str) -> Result<(Credentials, Option<Policy>)> {
        let (mut da, policy) = self.get_service_account_internal(access_key).await?;

        da.credentials.secret_key.clear();
        da.credentials.session_token.clear();

        Ok((da.credentials, policy))
    }

    async fn get_service_account_internal(&self, access_key: &str) -> Result<(UserIdentity, Option<Policy>)> {
        let (sa, claims) = match self.get_account_with_claims_allow_missing_exp(access_key).await {
            Ok(res) => res,
            Err(err) => {
                if is_err_no_such_account(&err) {
                    return Err(IamError::NoSuchServiceAccount(access_key.to_string()));
                }

                return Err(err);
            }
        };

        if !sa.credentials.is_service_account() {
            return Err(IamError::NoSuchServiceAccount(access_key.to_string()));
        }

        let op_pt = claims.get(&iam_policy_claim_name_sa());
        let op_sp = claims.get(SESSION_POLICY_NAME);
        if let (Some(pt), Some(sp)) = (op_pt, op_sp)
            && pt == EMBEDDED_POLICY_TYPE
        {
            let policy =
                serde_json::from_slice(&base64_simd::URL_SAFE_NO_PAD.decode_to_vec(sp.as_str().unwrap_or_default().as_bytes())?)?;
            return Ok((sa, Some(policy)));
        }

        Ok((sa, None))
    }

    async fn get_account_with_claims(&self, access_key: &str) -> Result<(UserIdentity, HashMap<String, Value>)> {
        let Some(acc) = self.store.get_user(access_key).await else {
            return Err(IamError::NoSuchAccount(access_key.to_string()));
        };

        let m = extract_jwt_claims(&acc)?;

        Ok((acc, m))
    }

    async fn get_account_with_claims_allow_missing_exp(
        &self,
        access_key: &str,
    ) -> Result<(UserIdentity, HashMap<String, Value>)> {
        let Some(acc) = self.store.get_user(access_key).await else {
            return Err(IamError::NoSuchAccount(access_key.to_string()));
        };

        let m = crate::manager::extract_jwt_claims_allow_missing_exp(&acc)?;

        Ok((acc, m))
    }

    pub async fn get_temporary_account(&self, access_key: &str) -> Result<(Credentials, Option<Policy>)> {
        let (mut sa, policy) = match self.get_temp_account(access_key).await {
            Ok(res) => res,
            Err(err) => {
                if is_err_no_such_temp_account(&err) {
                    // TODO: load_user
                    match self.get_temp_account(access_key).await {
                        Ok(res) => res,
                        Err(err) => return Err(err),
                    };
                }

                return Err(err);
            }
        };
        sa.credentials.secret_key.clear();
        sa.credentials.session_token.clear();

        Ok((sa.credentials, policy))
    }

    async fn get_temp_account(&self, access_key: &str) -> Result<(UserIdentity, Option<Policy>)> {
        let (sa, claims) = match self.get_account_with_claims(access_key).await {
            Ok(res) => res,
            Err(err) => {
                if is_err_no_such_account(&err) {
                    return Err(IamError::NoSuchTempAccount(access_key.to_string()));
                }

                return Err(err);
            }
        };

        if !sa.credentials.is_temp() {
            return Err(IamError::NoSuchTempAccount(access_key.to_string()));
        }

        let op_pt = claims.get(&iam_policy_claim_name_sa());
        let op_sp = claims.get(SESSION_POLICY_NAME);
        if let (Some(pt), Some(sp)) = (op_pt, op_sp)
            && pt == EMBEDDED_POLICY_TYPE
        {
            let policy =
                serde_json::from_slice(&base64_simd::URL_SAFE_NO_PAD.decode_to_vec(sp.as_str().unwrap_or_default().as_bytes())?)?;
            return Ok((sa, Some(policy)));
        }

        Ok((sa, None))
    }

    pub async fn get_claims_for_svc_acc(&self, access_key: &str) -> Result<HashMap<String, Value>> {
        let Some(u) = self.store.get_user(access_key).await else {
            return Err(IamError::NoSuchServiceAccount(access_key.to_string()));
        };

        if !u.credentials.is_service_account() {
            return Err(IamError::NoSuchServiceAccount(access_key.to_string()));
        }

        crate::manager::extract_jwt_claims_allow_missing_exp(&u)
    }

    pub async fn delete_service_account(&self, access_key: &str, notify: bool) -> Result<()> {
        let Some(u) = self.store.get_user(access_key).await else {
            return Ok(());
        };

        if !u.credentials.is_service_account() {
            return Ok(());
        }

        self.store.delete_user(access_key, UserType::Svc).await?;

        if notify
            && !self.has_watcher()
            && let Some(notification_sys) = get_global_notification_sys()
        {
            let resp = notification_sys.delete_service_account(access_key).await;
            for r in resp {
                if let Some(err) = r.err {
                    warn!("notify delete_service_account failed: {}", err);
                }
            }
        }

        Ok(())
    }

    async fn notify_for_group(&self, group: &str) {
        if self.has_watcher() {
            return;
        }

        // Fire-and-forget notification to peers - don't block group operations
        let group = group.to_string();
        tokio::spawn(async move {
            if let Some(notification_sys) = get_global_notification_sys() {
                let resp = notification_sys.load_group(&group).await;
                for r in resp {
                    if let Some(err) = r.err {
                        warn!("notify load_group failed (non-blocking): {}", err);
                    }
                }
            }
        });
    }

    pub async fn create_user(&self, access_key: &str, args: &AddOrUpdateUserReq) -> Result<OffsetDateTime> {
        if !is_access_key_valid(access_key) {
            return Err(IamError::InvalidAccessKeyLength);
        }

        if contains_reserved_chars(access_key) {
            return Err(IamError::ContainsReservedChars);
        }

        if !is_secret_key_valid(&args.secret_key) {
            return Err(IamError::InvalidSecretKeyLength);
        }

        let updated_at = self.store.add_user(access_key, args).await?;
        self.load_user(access_key, UserType::Reg).await?;

        self.notify_for_user(access_key, false).await;

        Ok(updated_at)
    }

    pub async fn set_user_secret_key(&self, access_key: &str, secret_key: &str) -> Result<()> {
        if !is_access_key_valid(access_key) {
            return Err(IamError::InvalidAccessKeyLength);
        }

        if !is_secret_key_valid(secret_key) {
            return Err(IamError::InvalidSecretKeyLength);
        }

        self.store.update_user_secret_key(access_key, secret_key).await
    }

    /// Add SSH public key for a user (for SFTP authentication)
    pub async fn add_user_ssh_public_key(&self, access_key: &str, public_key: &str) -> Result<()> {
        if !is_access_key_valid(access_key) {
            return Err(IamError::InvalidAccessKeyLength);
        }

        if public_key.is_empty() {
            return Err(IamError::InvalidArgument);
        }

        self.store.add_user_ssh_public_key(access_key, public_key).await
    }

    pub async fn check_key(&self, access_key: &str) -> Result<(Option<UserIdentity>, bool)> {
        if let Some(sys_cred) = get_global_action_cred()
            && sys_cred.access_key == access_key
        {
            return Ok((Some(UserIdentity::new(sys_cred)), true));
        }

        match self.store.get_user(access_key).await {
            Some(res) => {
                let ok = res.credentials.is_valid();

                Ok((Some(res), ok))
            }
            None => {
                match self.store.load_user(access_key).await {
                    Ok(()) => {}
                    Err(err) if is_err_no_such_user(&err) => {}
                    Err(err) => return Err(err),
                }

                if let Some(res) = self.store.get_user(access_key).await {
                    let ok = res.credentials.is_valid();

                    Ok((Some(res), ok))
                } else {
                    Ok((None, false))
                }
            }
        }
    }

    pub async fn get_user(&self, access_key: &str) -> Option<UserIdentity> {
        match self.check_key(access_key).await {
            Ok((u, _)) => u,
            _ => None,
        }
    }

    pub async fn add_users_to_group(&self, group: &str, users: Vec<String>) -> Result<OffsetDateTime> {
        if contains_reserved_chars(group) {
            return Err(IamError::GroupNameContainsReservedChars);
        }
        let updated_at = self.store.add_users_to_group(group, users).await?;

        self.notify_for_group(group).await;

        Ok(updated_at)
    }

    pub async fn remove_users_from_group(&self, group: &str, users: Vec<String>) -> Result<OffsetDateTime> {
        let updated_at = self.store.remove_users_from_group(group, users).await?;

        self.notify_for_group(group).await;

        Ok(updated_at)
    }

    pub async fn set_group_status(&self, group: &str, enable: bool) -> Result<OffsetDateTime> {
        let updated_at = self.store.set_group_status(group, enable).await?;

        self.notify_for_group(group).await;

        Ok(updated_at)
    }
    pub async fn get_group_description(&self, group: &str) -> Result<GroupDesc> {
        self.store.get_group_description(group).await
    }

    pub async fn list_groups_load(&self) -> Result<Vec<String>> {
        self.store.update_groups().await
    }

    pub async fn list_groups(&self) -> Result<Vec<String>> {
        self.store.list_groups().await
    }

    pub async fn policy_db_set(&self, name: &str, user_type: UserType, is_group: bool, policy: &str) -> Result<OffsetDateTime> {
        let updated_at = self.store.policy_db_set(name, user_type, is_group, policy).await?;

        if !self.has_watcher()
            && let Some(notification_sys) = get_global_notification_sys()
        {
            let resp = notification_sys.load_policy_mapping(name, user_type.to_u64(), is_group).await;
            for r in resp {
                if let Some(err) = r.err {
                    warn!("notify load_policy failed: {}", err);
                }
            }
        }

        Ok(updated_at)
    }

    pub async fn policy_db_get(&self, name: &str, groups: &Option<Vec<String>>) -> Result<Vec<String>> {
        self.store.policy_db_get(name, groups).await
    }

    fn is_safe_claim_policy_name(policy: &str) -> bool {
        !policy.is_empty() && policy.chars().all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    }

    /// Compatibility wrapper for service-account authorization entry points.
    /// The canonical evaluation path is `prepare_service_account_auth + eval_prepared`.
    pub async fn is_allowed_service_account(&self, args: &Args<'_>, parent_user: &str) -> bool {
        let prepared = self.prepare_service_account_auth(args, parent_user).await;
        self.eval_prepared(&prepared, args).await
    }

    pub async fn get_combined_policy(&self, policies: &[String]) -> Policy {
        self.store.merge_policies(&policies.join(",")).await.1
    }

    /// Prepare IAM authorization context once so callers can:
    /// 1) know whether policy evaluation may need `s3:ExistingObjectTag`, and
    /// 2) evaluate with final conditions without re-merging identity policies.
    pub async fn prepare_auth(&self, args: &Args<'_>) -> PreparedIamAuth {
        if args.is_owner {
            return PreparedIamAuth {
                needs_existing_object_tag: false,
                mode: PreparedIamMode::Owner,
            };
        }

        if Self::get_policy_plugin_client().await.is_some() {
            return PreparedIamAuth {
                needs_existing_object_tag: false,
                mode: PreparedIamMode::Opa,
            };
        }

        let Ok((is_temp, parent_user)) = self.is_temp_user(args.account).await else {
            return PreparedIamAuth {
                needs_existing_object_tag: false,
                mode: PreparedIamMode::Deny,
            };
        };
        if is_temp {
            return self.prepare_sts_auth(args, &parent_user).await;
        }

        let Ok((is_svc, parent_user)) = self.is_service_account(args.account).await else {
            return PreparedIamAuth {
                needs_existing_object_tag: false,
                mode: PreparedIamMode::Deny,
            };
        };
        if is_svc {
            return self.prepare_service_account_auth(args, &parent_user).await;
        }

        self.prepare_regular_auth(args).await
    }

    pub async fn eval_prepared(&self, prepared: &PreparedIamAuth, args: &Args<'_>) -> bool {
        match &prepared.mode {
            PreparedIamMode::Opa => {
                let Some(opa_enable) = Self::get_policy_plugin_client().await else {
                    tracing::warn!("eval_prepared: OPA mode requested but plugin is unavailable");
                    return false;
                };
                opa_enable.is_allowed(args).await
            }
            PreparedIamMode::Owner => true,
            PreparedIamMode::Deny => false,
            PreparedIamMode::Regular { combined_policy } => combined_policy.is_allowed(args).await,
            PreparedIamMode::Sts {
                is_owner,
                combined_policy,
                session_policy,
            } => {
                let session_ok = evaluate_prepared_session_policy(session_policy, args).await;
                if let Some(ok) = session_ok {
                    return ok && (*is_owner || combined_policy.is_allowed(args).await);
                }
                *is_owner || combined_policy.is_allowed(args).await
            }
            PreparedIamMode::ServiceAccount {
                is_owner,
                parent_user,
                combined_policy,
                mode,
                session_policy,
            } => {
                let mut parent_args = args.clone();
                parent_args.account = parent_user;

                let parent_allowed = *is_owner || combined_policy.is_allowed(&parent_args).await;
                match mode {
                    PreparedServicePolicyMode::Inherited => parent_allowed,
                    PreparedServicePolicyMode::SessionBound => {
                        let session_ok = evaluate_prepared_session_policy(session_policy, args).await;
                        if let Some(ok) = session_ok {
                            return ok && parent_allowed;
                        }
                        parent_allowed
                    }
                }
            }
        }
    }

    async fn prepare_regular_auth(&self, args: &Args<'_>) -> PreparedIamAuth {
        let Ok(policies) = self.policy_db_get(args.account, args.groups).await else {
            return PreparedIamAuth {
                needs_existing_object_tag: false,
                mode: PreparedIamMode::Deny,
            };
        };

        if policies.is_empty() {
            return PreparedIamAuth {
                needs_existing_object_tag: false,
                mode: PreparedIamMode::Deny,
            };
        }

        let combined_policy = self.get_combined_policy(&policies).await;
        PreparedIamAuth {
            needs_existing_object_tag: policy_needs_existing_object_tag_for_args(&combined_policy, args).await,
            mode: PreparedIamMode::Regular { combined_policy },
        }
    }

    pub(crate) async fn prepare_sts_auth(&self, args: &Args<'_>, parent_user: &str) -> PreparedIamAuth {
        let is_owner = matches!(get_global_action_cred(), Some(cred) if cred.access_key == parent_user);
        let role_arn = args.get_role_arn();

        let (effective_groups, groups_source, policies) = if is_owner {
            (None, "owner", Vec::new())
        } else if let Some(arn_str) = role_arn {
            let Ok(arn) = ARN::parse(arn_str) else {
                tracing::warn!(
                    parent_user = %parent_user,
                    role_arn = %arn_str,
                    "prepare_sts_auth: invalid role ARN in STS claims"
                );
                return PreparedIamAuth {
                    needs_existing_object_tag: false,
                    mode: PreparedIamMode::Deny,
                };
            };
            let p = MappedPolicy::new(self.roles_map.get(&arn).map_or_else(String::default, |v| v.clone()).as_str()).to_slice();
            (None, "role", p)
        } else {
            let (effective_groups, groups_source) = match args.groups.as_ref() {
                Some(g) if !g.is_empty() => (args.groups.clone(), "args"),
                _ => match self.store.get_user(parent_user).await {
                    Some(u) => (u.credentials.groups, "parent_user_credentials"),
                    None => {
                        tracing::warn!(
                            parent_user = %parent_user,
                            "prepare_sts_auth: groups fallback failed, parent user not found"
                        );
                        (None, "parent_user_credentials")
                    }
                },
            };
            let p = self.policy_db_get(parent_user, &effective_groups).await.unwrap_or_default();
            (effective_groups, groups_source, p)
        };

        let mut combined_policy = Policy::default();

        if !is_owner && policies.is_empty() {
            // For OIDC/STS users, policies may be specified in JWT claims rather than IAM DB.
            if let Some(claim_policies) = args.claims.get("policy").and_then(|v| v.as_str()) {
                use rustfs_policy::policy::default::DEFAULT_POLICIES;
                let mut resolved = Vec::new();
                for policy_name in claim_policies.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
                    if !Self::is_safe_claim_policy_name(policy_name) {
                        continue;
                    }
                    for (name, p) in DEFAULT_POLICIES.iter() {
                        if *name == policy_name {
                            resolved.push(p.clone());
                            break;
                        }
                    }
                }
                if !resolved.is_empty() {
                    combined_policy = Policy::merge_policies(resolved);
                } else if args.deny_only {
                    combined_policy = Policy::default();
                } else {
                    return PreparedIamAuth {
                        needs_existing_object_tag: false,
                        mode: PreparedIamMode::Deny,
                    };
                }
            } else if args.deny_only {
                combined_policy = Policy::default();
            } else {
                return PreparedIamAuth {
                    needs_existing_object_tag: false,
                    mode: PreparedIamMode::Deny,
                };
            }
        } else if !is_owner {
            let (a, c) = self.store.merge_policies(&policies.join(",")).await;
            if a.is_empty() {
                if args.deny_only {
                    combined_policy = Policy::default();
                } else {
                    return PreparedIamAuth {
                        needs_existing_object_tag: false,
                        mode: PreparedIamMode::Deny,
                    };
                }
            } else {
                combined_policy = c;
            }
        }

        let session_policy = prepare_session_policy(args, false);
        tracing::debug!(
            "prepare_sts_auth: action={:?}, is_owner={}, parent_user={}, groups_source={}, effective_groups={:?}",
            args.action,
            is_owner,
            parent_user,
            groups_source,
            effective_groups
        );
        PreparedIamAuth {
            needs_existing_object_tag: policy_needs_existing_object_tag_for_args(&combined_policy, args).await
                || prepared_session_policy_needs_existing_object_tag_for_args(&session_policy, args).await,
            mode: PreparedIamMode::Sts {
                is_owner,
                combined_policy,
                session_policy,
            },
        }
    }

    async fn prepare_service_account_auth(&self, args: &Args<'_>, parent_user: &str) -> PreparedIamAuth {
        let Some(p) = args.claims.get("parent") else {
            return PreparedIamAuth {
                needs_existing_object_tag: false,
                mode: PreparedIamMode::Deny,
            };
        };

        if p.as_str() != Some(parent_user) {
            return PreparedIamAuth {
                needs_existing_object_tag: false,
                mode: PreparedIamMode::Deny,
            };
        }

        let is_owner = matches!(get_global_action_cred(), Some(cred) if cred.access_key == parent_user);
        let role_arn = args.get_role_arn();

        let svc_policies = if is_owner {
            Vec::new()
        } else if role_arn.is_some() {
            let Ok(arn) = ARN::parse(role_arn.unwrap_or_default()) else {
                tracing::warn!(
                    parent_user = %parent_user,
                    role_arn = ?role_arn,
                    "prepare_service_account_auth: invalid role ARN in service account claims"
                );
                return PreparedIamAuth {
                    needs_existing_object_tag: false,
                    mode: PreparedIamMode::Deny,
                };
            };
            MappedPolicy::new(self.roles_map.get(&arn).map_or_else(String::default, |v| v.clone()).as_str()).to_slice()
        } else {
            let Ok(policies) = self.policy_db_get(parent_user, args.groups).await else {
                return PreparedIamAuth {
                    needs_existing_object_tag: false,
                    mode: PreparedIamMode::Deny,
                };
            };
            policies
        };

        if !is_owner && svc_policies.is_empty() {
            return PreparedIamAuth {
                needs_existing_object_tag: false,
                mode: PreparedIamMode::Deny,
            };
        }

        let combined_policy = if is_owner {
            Policy::default()
        } else {
            let (a, c) = self.store.merge_policies(&svc_policies.join(",")).await;
            if a.is_empty() {
                return PreparedIamAuth {
                    needs_existing_object_tag: false,
                    mode: PreparedIamMode::Deny,
                };
            }
            c
        };

        let Some(sa) = args.claims.get(&iam_policy_claim_name_sa()) else {
            return PreparedIamAuth {
                needs_existing_object_tag: false,
                mode: PreparedIamMode::Deny,
            };
        };
        let Some(sa_str) = sa.as_str() else {
            return PreparedIamAuth {
                needs_existing_object_tag: false,
                mode: PreparedIamMode::Deny,
            };
        };

        let mode = if sa_str == INHERITED_POLICY_TYPE {
            PreparedServicePolicyMode::Inherited
        } else {
            PreparedServicePolicyMode::SessionBound
        };

        let session_policy = prepare_session_policy(args, true);
        let needs_existing_object_tag = policy_needs_existing_object_tag_for_args(&combined_policy, args).await
            || matches!(mode, PreparedServicePolicyMode::SessionBound)
                && prepared_session_policy_needs_existing_object_tag_for_args(&session_policy, args).await;

        PreparedIamAuth {
            needs_existing_object_tag,
            mode: PreparedIamMode::ServiceAccount {
                is_owner,
                parent_user: parent_user.to_string(),
                combined_policy,
                mode,
                session_policy,
            },
        }
    }

    pub async fn is_allowed(&self, args: &Args<'_>) -> bool {
        let prepared = self.prepare_auth(args).await;
        self.eval_prepared(&prepared, args).await
    }

    /// Check if the underlying store is ready
    pub fn is_ready(&self) -> bool {
        self.store.is_ready()
    }
}

async fn prepared_session_policy_needs_existing_object_tag_for_args(policy: &PreparedSessionPolicy, args: &Args<'_>) -> bool {
    match policy {
        PreparedSessionPolicy::Policy(p) => policy_needs_existing_object_tag_for_args(p, args).await,
        PreparedSessionPolicy::None | PreparedSessionPolicy::DenyAll => false,
    }
}

fn prepare_session_policy(args: &Args<'_>, empty_is_none: bool) -> PreparedSessionPolicy {
    let Some(policy_str) = extract_session_policy_text(args.claims) else {
        return PreparedSessionPolicy::None;
    };

    let Ok(sub_policy) = Policy::parse_config(policy_str.as_bytes()) else {
        return PreparedSessionPolicy::DenyAll;
    };

    if empty_is_none {
        if sub_policy.version.is_empty() && sub_policy.statements.is_empty() && sub_policy.id.is_empty() {
            return PreparedSessionPolicy::None;
        }
        return PreparedSessionPolicy::Policy(sub_policy);
    }

    if sub_policy.version.is_empty() {
        return PreparedSessionPolicy::DenyAll;
    }

    PreparedSessionPolicy::Policy(sub_policy)
}

fn extract_session_policy_text(claims: &HashMap<String, Value>) -> Option<String> {
    if let Some(policy_str) = claims.get(SESSION_POLICY_NAME_EXTRACTED).and_then(|v| v.as_str()) {
        return Some(policy_str.to_string());
    }

    let encoded = claims.get(SESSION_POLICY_NAME).and_then(|v| v.as_str())?;
    let bytes = base64_simd::URL_SAFE_NO_PAD.decode_to_vec(encoded.as_bytes()).ok()?;
    String::from_utf8(bytes).ok()
}

async fn evaluate_prepared_session_policy(policy: &PreparedSessionPolicy, args: &Args<'_>) -> Option<bool> {
    match policy {
        PreparedSessionPolicy::None => None,
        PreparedSessionPolicy::DenyAll => Some(false),
        PreparedSessionPolicy::Policy(p) => {
            let mut session_policy_args = args.clone();
            session_policy_args.is_owner = false;
            Some(p.is_allowed(&session_policy_args).await)
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct NewServiceAccountOpts {
    pub session_policy: Option<Policy>,
    pub access_key: String,
    pub secret_key: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub expiration: Option<OffsetDateTime>,
    pub allow_site_replicator_account: bool,
    pub claims: Option<HashMap<String, Value>>,
}

pub struct UpdateServiceAccountOpts {
    pub session_policy: Option<Policy>,
    pub secret_key: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub expiration: Option<OffsetDateTime>,
    pub status: Option<String>,
}

pub fn get_claims_from_token_with_secret(token: &str, secret: &str) -> Result<HashMap<String, Value>> {
    let mut ms =
        extract_claims::<HashMap<String, Value>>(token, secret).map_err(|e| Error::other(format!("extract claims err {e}")))?;

    if let Some(session_policy) = ms.claims.get(SESSION_POLICY_NAME) {
        let policy_str = session_policy.as_str().unwrap_or_default();
        let policy = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(policy_str.as_bytes())
            .map_err(|e| Error::other(format!("base64 decode err {e}")))?;
        ms.claims.insert(
            SESSION_POLICY_NAME_EXTRACTED.to_string(),
            Value::String(String::from_utf8(policy).map_err(|e| Error::other(format!("utf8 decode err {e}")))?),
        );
    }
    Ok(ms.claims)
}

pub fn get_claims_from_token_with_secret_allow_missing_exp(token: &str, secret: &str) -> Result<HashMap<String, Value>> {
    let mut ms = extract_claims_allow_missing_exp::<HashMap<String, Value>>(token, secret)
        .map_err(|e| Error::other(format!("extract claims err {e}")))?;

    if let Some(session_policy) = ms.claims.get(SESSION_POLICY_NAME) {
        let policy_str = session_policy.as_str().unwrap_or_default();
        let policy = base64_simd::URL_SAFE_NO_PAD
            .decode_to_vec(policy_str.as_bytes())
            .map_err(|e| Error::other(format!("base64 decode err {e}")))?;
        ms.claims.insert(
            SESSION_POLICY_NAME_EXTRACTED.to_string(),
            Value::String(String::from_utf8(policy).map_err(|e| Error::other(format!("utf8 decode err {e}")))?),
        );
    }
    Ok(ms.claims)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{Cache, CacheEntity};
    use crate::error::Error;
    use crate::manager::get_default_policyes;
    use crate::store::{GroupInfo, MappedPolicy, Store, UserType};
    use rustfs_credentials::{Credentials, get_global_action_cred, init_global_action_credentials};
    use rustfs_policy::auth::UserIdentity;
    use rustfs_policy::policy::Args;
    use rustfs_policy::policy::action::{Action, AdminAction, S3Action};
    use rustfs_policy::policy::policy_uses_existing_object_tag_conditions;
    use serde_json::Value;
    use std::collections::HashMap;
    use time::OffsetDateTime;

    #[test]
    fn test_combined_policy_for_view_returns_regular_policy() {
        let policy = Policy {
            version: "2012-10-17".to_string(),
            ..Default::default()
        };
        let prepared = PreparedIamAuth {
            needs_existing_object_tag: false,
            mode: PreparedIamMode::Regular { combined_policy: policy },
        };

        let resolved = prepared.combined_policy_for_view();
        assert_eq!(resolved.map(|p| p.version.as_str()), Some("2012-10-17"));
    }

    #[test]
    fn test_combined_policy_for_view_returns_none_for_deny() {
        let prepared = PreparedIamAuth {
            needs_existing_object_tag: false,
            mode: PreparedIamMode::Deny,
        };

        assert!(prepared.combined_policy_for_view().is_none());
    }

    /// Mock Store for STS tests: either group-attached policies via parent user, or no IAM policies.
    #[derive(Clone)]
    struct StsTestMockStore {
        /// When true, parent user has no groups and no mapped policies (empty `policy_db_get`).
        empty_policies: bool,
    }

    #[async_trait::async_trait]
    impl Store for StsTestMockStore {
        fn has_watcher(&self) -> bool {
            false
        }

        async fn save_iam_config<Item: serde::Serialize + Send>(&self, _item: Item, _path: impl AsRef<str> + Send) -> Result<()> {
            Ok(())
        }

        async fn load_iam_config<Item: serde::de::DeserializeOwned>(&self, _path: impl AsRef<str> + Send) -> Result<Item> {
            Err(Error::ConfigNotFound)
        }

        async fn delete_iam_config(&self, _path: impl AsRef<str> + Send) -> Result<()> {
            Err(Error::InvalidArgument)
        }

        async fn save_user_identity(
            &self,
            _name: &str,
            _user_type: UserType,
            _item: UserIdentity,
            _ttl: Option<usize>,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_user_identity(&self, _name: &str, _user_type: UserType) -> Result<()> {
            Err(Error::InvalidArgument)
        }

        async fn load_user_identity(&self, _name: &str, _user_type: UserType) -> Result<UserIdentity> {
            Err(Error::InvalidArgument)
        }

        async fn load_user(&self, name: &str, user_type: UserType, m: &mut HashMap<String, UserIdentity>) -> Result<()> {
            if user_type == UserType::Reg && name == "load-failure-user" {
                return Err(Error::Io(std::io::Error::other("load user failed")));
            }

            if user_type == UserType::Reg && name == "notify-user" {
                let user = UserIdentity::from(Credentials {
                    access_key: name.to_string(),
                    secret_key: "notify-user-secret".to_string(),
                    status: ACCOUNT_ON.to_string(),
                    ..Default::default()
                });
                m.insert(name.to_string(), user);
            }
            Ok(())
        }

        async fn load_users(&self, _user_type: UserType, _m: &mut HashMap<String, UserIdentity>) -> Result<()> {
            Ok(())
        }

        async fn load_secret_key(&self, _name: &str, _user_type: UserType) -> Result<String> {
            Err(Error::InvalidArgument)
        }

        async fn save_group_info(&self, _name: &str, _item: GroupInfo) -> Result<()> {
            Err(Error::InvalidArgument)
        }

        async fn delete_group_info(&self, _name: &str) -> Result<()> {
            Err(Error::InvalidArgument)
        }

        async fn load_group(&self, _name: &str, _m: &mut HashMap<String, GroupInfo>) -> Result<()> {
            Ok(())
        }

        async fn load_groups(&self, _m: &mut HashMap<String, GroupInfo>) -> Result<()> {
            Err(Error::InvalidArgument)
        }

        async fn save_policy_doc(&self, _name: &str, _item: rustfs_policy::policy::PolicyDoc) -> Result<()> {
            Err(Error::InvalidArgument)
        }

        async fn delete_policy_doc(&self, _name: &str) -> Result<()> {
            Err(Error::InvalidArgument)
        }

        async fn load_policy(&self, _name: &str) -> Result<rustfs_policy::policy::PolicyDoc> {
            Err(Error::InvalidArgument)
        }

        async fn load_policy_doc(&self, _name: &str, _m: &mut HashMap<String, rustfs_policy::policy::PolicyDoc>) -> Result<()> {
            Err(Error::InvalidArgument)
        }

        async fn load_policy_docs(&self, _m: &mut HashMap<String, rustfs_policy::policy::PolicyDoc>) -> Result<()> {
            Err(Error::InvalidArgument)
        }

        async fn save_mapped_policy(
            &self,
            _name: &str,
            _user_type: UserType,
            _is_group: bool,
            _item: MappedPolicy,
            _ttl: Option<usize>,
        ) -> Result<()> {
            Err(Error::InvalidArgument)
        }

        async fn delete_mapped_policy(&self, _name: &str, _user_type: UserType, _is_group: bool) -> Result<()> {
            Err(Error::InvalidArgument)
        }

        async fn load_mapped_policy(
            &self,
            name: &str,
            user_type: UserType,
            is_group: bool,
            m: &mut HashMap<String, MappedPolicy>,
        ) -> Result<()> {
            if user_type == UserType::Reg && !is_group && name == "notify-user" {
                m.insert(name.to_string(), MappedPolicy::new("readwrite"));
            }
            Ok(())
        }

        async fn load_mapped_policies(
            &self,
            _user_type: UserType,
            _is_group: bool,
            _m: &mut HashMap<String, MappedPolicy>,
        ) -> Result<()> {
            Ok(())
        }

        async fn load_all(&self, cache: &Cache) -> Result<()> {
            let policy_docs = get_default_policyes();
            cache
                .policy_docs
                .store(Arc::new(CacheEntity::new(policy_docs).update_load_time()));

            if self.empty_policies {
                const PARENT_USER: &str = "sts-empty-parent-policy-test";
                let creds = Credentials {
                    access_key: PARENT_USER.to_string(),
                    secret_key: "longenoughsecret".to_string(),
                    session_token: String::new(),
                    expiration: None,
                    status: "on".to_string(),
                    parent_user: String::new(),
                    groups: None,
                    claims: None,
                    name: None,
                    description: None,
                };
                let parent_identity = UserIdentity {
                    version: 1,
                    credentials: creds,
                    update_at: Some(OffsetDateTime::now_utc()),
                };
                let mut users = HashMap::new();
                users.insert(PARENT_USER.to_string(), parent_identity);
                cache.users.store(Arc::new(CacheEntity::new(users).update_load_time()));

                cache.groups.store(Arc::new(CacheEntity::default().update_load_time()));
                cache
                    .group_policies
                    .store(Arc::new(CacheEntity::default().update_load_time()));
                cache.user_policies.store(Arc::new(CacheEntity::default().update_load_time()));
                cache.sts_accounts.store(Arc::new(CacheEntity::default().update_load_time()));
                cache.sts_policies.store(Arc::new(CacheEntity::default().update_load_time()));
                cache.build_user_group_memberships();
                return Ok(());
            }

            const PARENT_USER: &str = "sts-fallback-test-parent";
            const GROUP_NAME: &str = "testgroup";

            let creds = Credentials {
                access_key: PARENT_USER.to_string(),
                secret_key: "longenoughsecret".to_string(),
                session_token: String::new(),
                expiration: None,
                status: "on".to_string(),
                parent_user: String::new(),
                groups: Some(vec![GROUP_NAME.to_string()]),
                claims: None,
                name: None,
                description: None,
            };
            let parent_identity = UserIdentity {
                version: 1,
                credentials: creds,
                update_at: Some(OffsetDateTime::now_utc()),
            };
            let mut users = HashMap::new();
            users.insert(PARENT_USER.to_string(), parent_identity);
            cache.users.store(Arc::new(CacheEntity::new(users).update_load_time()));

            let group = GroupInfo::new(vec![PARENT_USER.to_string()]);
            let mut groups = HashMap::new();
            groups.insert(GROUP_NAME.to_string(), group);
            cache.groups.store(Arc::new(CacheEntity::new(groups).update_load_time()));

            let group_policy = MappedPolicy::new("readwrite");
            let mut group_policies = HashMap::new();
            group_policies.insert(GROUP_NAME.to_string(), group_policy);
            cache
                .group_policies
                .store(Arc::new(CacheEntity::new(group_policies).update_load_time()));

            cache.user_policies.store(Arc::new(CacheEntity::default().update_load_time()));
            cache.sts_accounts.store(Arc::new(CacheEntity::default().update_load_time()));
            cache.sts_policies.store(Arc::new(CacheEntity::default().update_load_time()));
            cache.build_user_group_memberships();

            Ok(())
        }
    }

    fn ensure_test_global_credentials() {
        if get_global_action_cred().is_none() {
            let _ = init_global_action_credentials(Some("TESTROOTACCESSKEY".to_string()), Some("TESTROOTSECRET123".to_string()));
        }
    }

    #[tokio::test]
    async fn test_new_service_account_without_expiration_omits_exp_claim() {
        ensure_test_global_credentials();

        let store = StsTestMockStore { empty_policies: false };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        let (cred, _) = iam_sys
            .new_service_account("svc-parent-user", None, NewServiceAccountOpts::default())
            .await
            .expect("service account should be created without expiration");

        assert!(cred.expiration.is_none());

        let claims = get_claims_from_token_with_secret_allow_missing_exp(&cred.session_token, &cred.secret_key)
            .expect("service account JWT without expiration should decode");
        assert!(
            !claims.contains_key("exp"),
            "service account without explicit expiration should not get a default JWT exp"
        );
    }

    #[tokio::test]
    async fn test_update_service_account_updates_exp_claim() {
        ensure_test_global_credentials();

        let store = StsTestMockStore { empty_policies: false };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        let initial_expiration = OffsetDateTime::now_utc() + time::Duration::hours(2);
        let (cred, _) = iam_sys
            .new_service_account(
                "svc-parent-user",
                None,
                NewServiceAccountOpts {
                    expiration: Some(initial_expiration),
                    ..Default::default()
                },
            )
            .await
            .expect("service account with explicit expiration should be created");

        let updated_expiration = OffsetDateTime::now_utc() + time::Duration::hours(4);
        iam_sys
            .update_service_account(
                &cred.access_key,
                UpdateServiceAccountOpts {
                    session_policy: None,
                    secret_key: None,
                    name: None,
                    description: None,
                    expiration: Some(updated_expiration),
                    status: None,
                },
            )
            .await
            .expect("service account expiration should update");

        let updated_user = iam_sys
            .get_user(&cred.access_key)
            .await
            .expect("updated service account should exist");
        assert_eq!(updated_user.credentials.expiration, Some(updated_expiration));

        let claims =
            get_claims_from_token_with_secret(&updated_user.credentials.session_token, &updated_user.credentials.secret_key)
                .expect("updated service account JWT should decode");
        assert_eq!(
            claims.get("exp").and_then(|v| v.as_i64()),
            Some(updated_expiration.unix_timestamp()),
            "updating service account expiration must rewrite the JWT exp claim"
        );
    }

    #[tokio::test]
    async fn test_update_service_account_adds_exp_claim_to_non_expiring_account() {
        ensure_test_global_credentials();

        let store = StsTestMockStore { empty_policies: false };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        let (cred, _) = iam_sys
            .new_service_account("svc-parent-user", None, NewServiceAccountOpts::default())
            .await
            .expect("service account without explicit expiration should be created");

        let updated_expiration = OffsetDateTime::now_utc() + time::Duration::hours(3);
        iam_sys
            .update_service_account(
                &cred.access_key,
                UpdateServiceAccountOpts {
                    session_policy: None,
                    secret_key: None,
                    name: None,
                    description: None,
                    expiration: Some(updated_expiration),
                    status: None,
                },
            )
            .await
            .expect("service account without expiration should accept a new expiration");

        let updated_user = iam_sys
            .get_user(&cred.access_key)
            .await
            .expect("updated service account should exist");
        assert_eq!(updated_user.credentials.expiration, Some(updated_expiration));

        let claims =
            get_claims_from_token_with_secret(&updated_user.credentials.session_token, &updated_user.credentials.secret_key)
                .expect("updated service account JWT should decode after adding expiration");
        assert_eq!(claims.get("exp").and_then(|v| v.as_i64()), Some(updated_expiration.unix_timestamp()));
    }

    /// Regression test: temp credentials without groups in args still receive group-attached
    /// policies via the parent user (groups fallback). Without the fallback, policy_db_get
    /// would get None for groups and the user would have no group policies, so the action
    /// would be denied.
    #[tokio::test]
    async fn test_sts_groups_fallback_temp_creds_receive_parent_group_policies() {
        let store = StsTestMockStore { empty_policies: false };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        let parent_user = "sts-fallback-test-parent";
        let claims = HashMap::new();
        let groups: Option<Vec<String>> = None;
        let args = Args {
            account: parent_user,
            groups: &groups,
            action: Action::S3Action(S3Action::ListBucketAction),
            bucket: "mybucket",
            conditions: &HashMap::new(),
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };

        let prepared = iam_sys.prepare_sts_auth(&args, parent_user).await;
        let allowed = iam_sys.eval_prepared(&prepared, &args).await;
        assert!(
            allowed,
            "STS temp credentials with no groups in args should still be allowed via parent user's group policy (readwrite)"
        );
    }

    /// Regression: `deny_only` with empty IAM policies must still evaluate `sessionPolicy-extracted`
    /// so session policy Deny cannot be bypassed (see PR #2250 review).
    #[tokio::test]
    async fn test_sts_deny_only_session_policy_deny_blocks_when_iam_policies_empty() {
        let store = StsTestMockStore { empty_policies: true };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        let parent_user = "sts-empty-parent-policy-test";
        let session_policy_json = r#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Deny",
      "Action": ["admin:CreateUser"],
      "Resource": ["arn:aws:s3:::*"]
    }
  ]
}"#;
        let mut claims = HashMap::new();
        claims.insert(SESSION_POLICY_NAME_EXTRACTED.to_string(), Value::String(session_policy_json.to_string()));
        let groups: Option<Vec<String>> = None;
        let args = Args {
            account: parent_user,
            groups: &groups,
            action: Action::AdminAction(AdminAction::CreateUserAdminAction),
            bucket: "",
            conditions: &HashMap::new(),
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: true,
        };

        let prepared = iam_sys.prepare_sts_auth(&args, parent_user).await;
        let allowed = iam_sys.eval_prepared(&prepared, &args).await;
        assert!(
            !allowed,
            "session policy Deny must be evaluated even when IAM policies are empty and deny_only is set"
        );
    }

    #[tokio::test]
    async fn test_sts_deny_only_session_policy_allow_when_no_deny_on_action() {
        let store = StsTestMockStore { empty_policies: true };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        let parent_user = "sts-empty-parent-policy-test";
        let session_policy_json = r#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::bucket/*"]
    }
  ]
}"#;
        let mut claims = HashMap::new();
        claims.insert(SESSION_POLICY_NAME_EXTRACTED.to_string(), Value::String(session_policy_json.to_string()));
        let groups: Option<Vec<String>> = None;
        let args = Args {
            account: parent_user,
            groups: &groups,
            action: Action::AdminAction(AdminAction::CreateUserAdminAction),
            bucket: "",
            conditions: &HashMap::new(),
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: true,
        };

        let prepared = iam_sys.prepare_sts_auth(&args, parent_user).await;
        let allowed = iam_sys.eval_prepared(&prepared, &args).await;
        assert!(
            allowed,
            "deny_only with no matching Deny in session policy should still allow self-service-style checks"
        );
    }

    /// Regression test for cross-node IAM notifications:
    /// `load_user` must populate user cache, and regular-user mapped policy must be written to
    /// `user_policies` (not `sts_policies`), otherwise list-users and bucket-scoped user listing
    /// may miss users on follower nodes.
    #[tokio::test]
    async fn test_load_user_notification_populates_user_and_policy_caches() {
        let store = StsTestMockStore { empty_policies: false };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        iam_sys.load_user("notify-user", UserType::Reg).await.unwrap();

        let users = iam_sys.list_users().await.unwrap();
        assert!(
            users.contains_key("notify-user"),
            "regular user loaded via notification must appear in list_users cache view"
        );

        let bucket_users = iam_sys.list_bucket_users("notification-regression-bucket").await.unwrap();
        assert!(
            bucket_users.contains_key("notify-user"),
            "regular user mapped policy must be written to user_policies for bucket user listing"
        );
    }

    #[tokio::test]
    async fn test_check_key_propagates_cache_miss_load_failure() {
        let store = StsTestMockStore { empty_policies: false };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        let result = iam_sys.check_key("load-failure-user").await;

        assert!(matches!(result, Err(Error::Io(_))));
    }

    #[tokio::test]
    async fn test_prepare_auth_eval_matches_prepare_sts_auth_for_parent_policy_fallback() {
        let store = StsTestMockStore { empty_policies: false };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        let parent_user = "sts-fallback-test-parent";
        let claims = HashMap::new();
        let groups: Option<Vec<String>> = None;
        let args = Args {
            account: parent_user,
            groups: &groups,
            action: Action::S3Action(S3Action::ListBucketAction),
            bucket: "mybucket",
            conditions: &HashMap::new(),
            is_owner: false,
            object: "",
            claims: &claims,
            deny_only: false,
        };

        let sts_prepared = iam_sys.prepare_sts_auth(&args, parent_user).await;
        let sts_eval = iam_sys.eval_prepared(&sts_prepared, &args).await;
        let prepared = iam_sys.prepare_auth(&args).await;
        let eval = iam_sys.eval_prepared(&prepared, &args).await;
        assert_eq!(sts_eval, eval, "prepare_auth must match explicit STS preparation for this identity");
    }

    #[tokio::test]
    async fn test_prepare_auth_detects_existing_object_tag_in_session_policy() {
        let store = StsTestMockStore { empty_policies: true };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);
        let sts_access_key = "sts-session-tag-test-user";

        let sts_user = UserIdentity::from(Credentials {
            access_key: sts_access_key.to_string(),
            secret_key: "longenoughsecret".to_string(),
            session_token: "sts-token".to_string(),
            status: ACCOUNT_ON.to_string(),
            parent_user: "sts-empty-parent-policy-test".to_string(),
            ..Default::default()
        });
        Cache::add_or_update(&iam_sys.store.cache.sts_accounts, sts_access_key, &sts_user, OffsetDateTime::now_utc());

        let mut claims = HashMap::new();
        claims.insert(
            SESSION_POLICY_NAME_EXTRACTED.to_string(),
            Value::String(
                r#"{
  "Version":"2012-10-17",
  "Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/*"],"Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]
}"#
                .to_string(),
            ),
        );

        let groups: Option<Vec<String>> = None;
        let args = Args {
            account: sts_access_key,
            groups: &groups,
            action: Action::S3Action(S3Action::GetObjectAction),
            bucket: "bucket",
            conditions: &HashMap::new(),
            is_owner: false,
            object: "obj",
            claims: &claims,
            deny_only: true,
        };

        let prepared = iam_sys.prepare_auth(&args).await;
        assert!(
            prepared.needs_existing_object_tag,
            "session policy with ExistingObjectTag must request object tag loading"
        );
    }

    #[test]
    fn test_policy_uses_existing_object_tag_matches_condition_keys_only() {
        let with_value_only = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::bucket/*"],
    "Condition":{"StringEquals":{"s3:prefix":"ExistingObjectTag/security"}}
  }]
}"#,
        )
        .expect("policy with value-only ExistingObjectTag text should parse");
        assert!(
            !policy_uses_existing_object_tag_conditions(&with_value_only),
            "ExistingObjectTag text in values should not trigger tag dependency"
        );

        let with_condition_key = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[{
    "Effect":"Allow",
    "Action":["s3:GetObject"],
    "Resource":["arn:aws:s3:::bucket/*"],
    "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
  }]
}"#,
        )
        .expect("policy with ExistingObjectTag condition key should parse");
        assert!(
            policy_uses_existing_object_tag_conditions(&with_condition_key),
            "ExistingObjectTag condition key must trigger tag dependency"
        );
    }

    #[test]
    fn test_policy_uses_existing_object_tag_when_only_secondary_action_has_tag_condition() {
        let split_action_policy = Policy::parse_config(
            br#"{
  "Version":"2012-10-17",
  "Statement":[
    {
      "Effect":"Allow",
      "Action":["s3:DeleteObject"],
      "Resource":["arn:aws:s3:::bucket/*"]
    },
    {
      "Effect":"Allow",
      "Action":["s3:DeleteObjectVersion"],
      "Resource":["arn:aws:s3:::bucket/*"],
      "Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}
    }
  ]
}"#,
        )
        .expect("split-action policy should parse");

        assert!(
            policy_uses_existing_object_tag_conditions(&split_action_policy),
            "full merged policy must still be detectable as containing ExistingObjectTag keys"
        );
    }

    #[tokio::test]
    async fn test_prepare_auth_detects_existing_object_tag_in_encoded_session_policy() {
        let store = StsTestMockStore { empty_policies: true };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);
        let sts_access_key = "sts-session-tag-encoded-test-user";

        let sts_user = UserIdentity::from(Credentials {
            access_key: sts_access_key.to_string(),
            secret_key: "longenoughsecret".to_string(),
            session_token: "sts-token".to_string(),
            status: ACCOUNT_ON.to_string(),
            parent_user: "sts-empty-parent-policy-test".to_string(),
            ..Default::default()
        });
        Cache::add_or_update(&iam_sys.store.cache.sts_accounts, sts_access_key, &sts_user, OffsetDateTime::now_utc());

        let session_policy_json = r#"{
  "Version":"2012-10-17",
  "Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/*"],"Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]
}"#;
        let mut claims = HashMap::new();
        claims.insert(
            SESSION_POLICY_NAME.to_string(),
            Value::String(base64_simd::URL_SAFE_NO_PAD.encode_to_string(session_policy_json.as_bytes())),
        );

        let groups: Option<Vec<String>> = None;
        let args = Args {
            account: sts_access_key,
            groups: &groups,
            action: Action::S3Action(S3Action::GetObjectAction),
            bucket: "bucket",
            conditions: &HashMap::new(),
            is_owner: false,
            object: "obj",
            claims: &claims,
            deny_only: true,
        };

        let prepared = iam_sys.prepare_auth(&args).await;
        assert!(
            prepared.needs_existing_object_tag,
            "base64 sessionPolicy with ExistingObjectTag must request object tag loading"
        );
    }

    #[tokio::test]
    async fn test_prepare_auth_service_account_inherited_ignores_session_policy_tag_hint() {
        let store = StsTestMockStore { empty_policies: false };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        let service_account_access_key = "svc-inherited-tag-hint-test-user";
        let parent_user = "sts-fallback-test-parent";
        let mut service_account_claims = HashMap::new();
        service_account_claims.insert(iam_policy_claim_name_sa(), Value::String(INHERITED_POLICY_TYPE.to_string()));
        let service_identity = UserIdentity::from(Credentials {
            access_key: service_account_access_key.to_string(),
            secret_key: "longenoughsecret".to_string(),
            status: ACCOUNT_ON.to_string(),
            parent_user: parent_user.to_string(),
            claims: Some(service_account_claims),
            ..Default::default()
        });
        Cache::add_or_update(
            &iam_sys.store.cache.users,
            service_account_access_key,
            &service_identity,
            OffsetDateTime::now_utc(),
        );

        let mut request_claims = HashMap::new();
        request_claims.insert("parent".to_string(), Value::String(parent_user.to_string()));
        request_claims.insert(iam_policy_claim_name_sa(), Value::String(INHERITED_POLICY_TYPE.to_string()));
        request_claims.insert(
            SESSION_POLICY_NAME_EXTRACTED.to_string(),
            Value::String(
                r#"{
  "Version":"2012-10-17",
  "Statement":[{"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::bucket/*"],"Condition":{"StringEquals":{"s3:ExistingObjectTag/security":"public"}}}]
}"#
                .to_string(),
            ),
        );

        let groups: Option<Vec<String>> = Some(vec!["testgroup".to_string()]);
        let args = Args {
            account: service_account_access_key,
            groups: &groups,
            action: Action::S3Action(S3Action::GetObjectAction),
            bucket: "bucket",
            conditions: &HashMap::new(),
            is_owner: false,
            object: "obj",
            claims: &request_claims,
            deny_only: false,
        };

        let prepared = iam_sys.prepare_auth(&args).await;
        assert!(
            !prepared.needs_existing_object_tag,
            "inherited service account should not require object tag fetch based on session policy hint"
        );
    }

    /// Regression test for rustfs#2392: `policy_db_get` must skip non-existent groups
    /// instead of aborting the entire policy resolution. When a JWT contains groups
    /// that exist in the IdP but not in IAM, policies from the remaining valid groups
    /// must still be returned.
    #[tokio::test]
    async fn test_policy_db_get_skips_nonexistent_groups() {
        let store = StsTestMockStore { empty_policies: false };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        // "testgroup" exists with "readwrite" policy; "nonexistent-group" does not exist in IAM.
        let groups = Some(vec!["testgroup".to_string(), "nonexistent-group".to_string()]);

        let policies = iam_sys
            .policy_db_get("sts-fallback-test-parent", &groups)
            .await
            .expect("policy_db_get should not fail when some groups are missing");

        assert!(
            policies.iter().any(|p| p == "readwrite"),
            "policies from existing group 'testgroup' should be returned even when other groups are missing; got: {:?}",
            policies
        );
    }

    #[tokio::test]
    async fn test_info_policy_returns_policy_as_json_object() {
        let store = StsTestMockStore { empty_policies: false };
        let cache_manager = IamCache::new(store).await;
        let iam_sys = IamSys::new(cache_manager);

        let policy_info = iam_sys
            .info_policy("readonly")
            .await
            .expect("info_policy should return existing default policy");

        assert!(
            policy_info.policy.is_object(),
            "policy field should be a JSON object for MinIO-compatible policy readback; got: {}",
            policy_info.policy
        );
        assert!(
            policy_info.policy.get("Version").is_some(),
            "policy object should contain Version field; got: {}",
            policy_info.policy
        );
        assert!(
            policy_info.policy.get("Statement").is_some(),
            "policy object should contain Statement field; got: {}",
            policy_info.policy
        );
    }
}
