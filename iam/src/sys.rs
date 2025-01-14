use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use crate::arn::ARN;
use crate::auth::contains_reserved_chars;
use crate::auth::create_new_credentials_with_metadata;
use crate::auth::generate_credentials;
use crate::auth::is_access_key_valid;
use crate::auth::is_secret_key_valid;
use crate::auth::Credentials;
use crate::auth::UserIdentity;
use crate::auth::ACCOUNT_ON;
use crate::error::is_err_no_such_account;
use crate::error::is_err_no_such_temp_account;
use crate::error::Error as IamError;
use crate::get_global_action_cred;
use crate::manager::extract_jwt_claims;
use crate::manager::get_default_policyes;
use crate::manager::IamCache;
use crate::policy::action::Action;
use crate::policy::Policy;
use crate::policy::PolicyDoc;
use crate::store::MappedPolicy;
use crate::store::Store;
use crate::store::UserType;
use ecstore::error::{Error, Result};
use ecstore::utils::crypto::base64_decode;
use ecstore::utils::crypto::base64_encode;
use madmin::GroupDesc;
use serde_json::json;
use serde_json::Value;
use time::OffsetDateTime;

pub const MAX_SVCSESSION_POLICY_SIZE: usize = 4096;

pub const STATUS_ENABLED: &str = "enabled";
pub const STATUS_DISABLED: &str = "disabled";

pub const POLICYNAME: &str = "policy";
pub const SESSION_POLICY_NAME: &str = "sessionPolicy";
pub const SESSION_POLICY_NAME_EXTRACTED: &str = "sessionPolicy-extracted";

pub const EMBEDDED_POLICY_TYPE: &str = "embedded-policy";
pub const INHERITED_POLICY_TYPE: &str = "inherited-policy";

pub struct IamSys<T> {
    store: Arc<IamCache<T>>,
    roles_map: HashMap<ARN, String>,
}

impl<T: Store> IamSys<T> {
    pub fn new(store: Arc<IamCache<T>>) -> Self {
        Self {
            store,
            roles_map: HashMap::new(),
        }
    }

    pub async fn load_group(&self, name: &str) -> Result<()> {
        self.store.group_notification_handler(name).await
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

    pub async fn load_service_account(&self, name: &str) -> Result<()> {
        self.store.user_notification_handler(name, UserType::Svc).await
    }

    pub async fn delete_policy(&self, name: &str, notify: bool) -> Result<()> {
        for k in get_default_policyes().keys() {
            if k == name {
                return Err(Error::msg("system policy can not be deleted"));
            }
        }

        self.store.delete_policy(name, notify).await?;

        if notify {
            // TODO: implement notification
        }

        Ok(())
    }

    pub async fn info_policy(&self, name: &str) -> Result<madmin::PolicyInfo> {
        let d = self.store.get_policy_doc(name).await?;

        let pdata = serde_json::to_string(&d.policy)?;

        Ok(madmin::PolicyInfo {
            policy_name: name.to_string(),
            policy: json!(pdata),
            create_date: d.create_date,
            update_date: d.update_date,
        })
    }

    pub async fn list_polices(&self, bucket_name: &str) -> Result<HashMap<String, Policy>> {
        self.store.list_polices(bucket_name).await
    }

    pub async fn list_policy_docs(&self, bucket_name: &str) -> Result<HashMap<String, PolicyDoc>> {
        self.store.list_policy_docs(bucket_name).await
    }

    pub async fn set_policy(&self, name: &str, policy: Policy) -> Result<OffsetDateTime> {
        self.store.set_policy(name, policy).await

        // TODO: notification
    }

    pub async fn delete_user(&self, name: &str, _notify: bool) -> Result<()> {
        self.store.delete_user(name, UserType::Reg).await
        // TODO: notification
    }

    pub async fn current_policies(&self, name: &str) -> String {
        self.store.merge_policies(name).await.0
    }

    pub async fn list_bucket_users(&self, bucket_name: &str) -> Result<HashMap<String, madmin::UserInfo>> {
        self.store.get_bucket_users(bucket_name).await
    }

    pub async fn list_users(&self) -> Result<HashMap<String, madmin::UserInfo>> {
        self.store.get_users().await
    }

    pub async fn set_temp_user(&self, name: &str, cred: &Credentials, policy_name: Option<&str>) -> Result<OffsetDateTime> {
        self.store.set_temp_user(name, cred, policy_name).await
        // TODO: notification
    }

    pub async fn is_temp_user(&self, name: &str) -> Result<(bool, String)> {
        let Some(u) = self.store.get_user(name).await else {
            return Err(IamError::NoSuchUser(name.to_string()).into());
        };
        if u.credentials.is_temp() {
            Ok((true, u.credentials.parent_user))
        } else {
            Ok((false, "".to_string()))
        }
    }
    pub async fn is_service_account(&self, name: &str) -> Result<(bool, String)> {
        let Some(u) = self.store.get_user(name).await else {
            return Err(IamError::NoSuchUser(name.to_string()).into());
        };

        if u.credentials.is_service_account() {
            Ok((true, u.credentials.parent_user))
        } else {
            Ok((false, "".to_string()))
        }
    }

    pub async fn get_user_info(&self, name: &str) -> Result<madmin::UserInfo> {
        self.store.get_user_info(name).await
    }

    pub async fn set_user_status(&self, name: &str, status: madmin::AccountStatus) -> Result<OffsetDateTime> {
        self.store.set_user_status(name, status).await
        // TODO: notification
    }

    pub async fn new_service_account(
        &self,
        parent_user: &str,
        groups: Vec<String>,
        opts: NewServiceAccountOpts,
    ) -> Result<OffsetDateTime> {
        if parent_user.is_empty() {
            return Err(IamError::InvalidArgument.into());
        }
        if !opts.access_key.is_empty() && opts.secret_key.is_empty() {
            return Err(IamError::NoSecretKeyWithAccessKey.into());
        }

        if !opts.secret_key.is_empty() && opts.access_key.is_empty() {
            return Err(IamError::NoAccessKeyWithSecretKey.into());
        }

        if parent_user == opts.access_key {
            return Err(IamError::IAMActionNotAllowed.into());
        }

        // TODO: check allow_site_replicator_account

        let policy_buf = if let Some(policy) = opts.session_policy {
            policy.validate()?;
            let buf = serde_json::to_vec(&policy)?;
            if buf.len() > MAX_SVCSESSION_POLICY_SIZE {
                return Err(IamError::PolicyTooLarge.into());
            }

            buf
        } else {
            Vec::new()
        };

        let mut m = HashMap::new();
        m.insert("parent".to_owned(), parent_user.to_owned());

        if !policy_buf.is_empty() {
            m.insert(SESSION_POLICY_NAME.to_owned(), base64_encode(&policy_buf));
            m.insert(iam_policy_claim_name_sa(), EMBEDDED_POLICY_TYPE.to_owned());
        } else {
            m.insert(iam_policy_claim_name_sa(), INHERITED_POLICY_TYPE.to_owned());
        }

        if let Some(claims) = opts.claims {
            for (k, v) in claims.iter() {
                if !m.contains_key(k) {
                    m.insert(k.to_owned(), v.to_owned());
                }
            }
        }

        let (access_key, secret_key) = if !opts.access_key.is_empty() || !opts.secret_key.is_empty() {
            (opts.access_key, opts.secret_key)
        } else {
            generate_credentials()?
        };

        let mut cred = create_new_credentials_with_metadata(&access_key, &secret_key, &m, &secret_key, None)?;
        cred.parent_user = parent_user.to_owned();
        cred.groups = Some(groups);
        cred.status = ACCOUNT_ON.to_owned();
        cred.name = opts.name;
        cred.description = opts.description;
        cred.expiration = opts.expiration;

        self.store.add_service_account(cred).await

        // TODO: notification
    }

    pub async fn update_service_account(&self, name: &str, opts: UpdateServiceAccountOpts) -> Result<OffsetDateTime> {
        self.store.update_service_account(name, opts).await

        // TODO: notification
    }

    pub async fn list_service_accounts(&self, access_key: &str) -> Result<Vec<Credentials>> {
        self.store.list_service_accounts(access_key).await
    }

    pub async fn list_tmep_accounts(&self, access_key: &str) -> Result<Vec<UserIdentity>> {
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
        let (sa, claims) = match self.get_account_with_claims(access_key).await {
            Ok(res) => res,
            Err(err) => {
                if is_err_no_such_account(&err) {
                    return Err(IamError::NoSuchServiceAccount(access_key.to_string()).into());
                }

                return Err(err);
            }
        };

        if !sa.credentials.is_service_account() {
            return Err(IamError::NoSuchServiceAccount(access_key.to_string()).into());
        }

        let op_pt = claims.get(&iam_policy_claim_name_sa());
        let op_sp = claims.get(SESSION_POLICY_NAME);
        if let (Some(pt), Some(sp)) = (op_pt, op_sp) {
            if pt == EMBEDDED_POLICY_TYPE {
                let policy = serde_json::from_slice(&base64_decode(sp.as_str().unwrap_or_default().as_bytes())?)?;
                return Ok((sa, Some(policy)));
            }
        }

        Ok((sa, None))
    }

    async fn get_account_with_claims(&self, access_key: &str) -> Result<(UserIdentity, HashMap<String, Value>)> {
        let Some(acc) = self.store.get_user(access_key).await else {
            return Err(IamError::NoSuchAccount(access_key.to_string()).into());
        };

        let m = extract_jwt_claims(&acc)?;

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
                    return Err(IamError::NoSuchTempAccount(access_key.to_string()).into());
                }

                return Err(err);
            }
        };

        if !sa.credentials.is_temp() {
            return Err(IamError::NoSuchTempAccount(access_key.to_string()).into());
        }

        let op_pt = claims.get(&iam_policy_claim_name_sa());
        let op_sp = claims.get(SESSION_POLICY_NAME);
        if let (Some(pt), Some(sp)) = (op_pt, op_sp) {
            if pt == EMBEDDED_POLICY_TYPE {
                let policy = serde_json::from_slice(&base64_decode(sp.as_str().unwrap_or_default().as_bytes())?)?;
                return Ok((sa, Some(policy)));
            }
        }

        Ok((sa, None))
    }

    pub async fn get_claims_for_svc_acc(&self, access_key: &str) -> Result<HashMap<String, Value>> {
        let Some(u) = self.store.get_user(access_key).await else {
            return Err(IamError::NoSuchServiceAccount(access_key.to_string()).into());
        };

        if u.credentials.is_service_account() {
            return Err(IamError::NoSuchServiceAccount(access_key.to_string()).into());
        }

        extract_jwt_claims(&u)
    }

    pub async fn delete_service_account(&self, access_key: &str) -> Result<()> {
        let Some(u) = self.store.get_user(access_key).await else {
            return Ok(());
        };

        if u.credentials.is_service_account() {
            return Ok(());
        }

        self.store.delete_user(access_key, UserType::Svc).await

        // TODO: notification
    }

    pub async fn create_user(&self, access_key: &str, secret_key: &str, status: &str) -> Result<OffsetDateTime> {
        if !is_access_key_valid(access_key) {
            return Err(IamError::InvalidAccessKeyLength.into());
        }

        if contains_reserved_chars(access_key) {
            return Err(IamError::ContainsReservedChars.into());
        }

        if !is_secret_key_valid(secret_key) {
            return Err(IamError::InvalidSecretKeyLength.into());
        }

        self.store.add_user(access_key, secret_key, status).await
        // TODO: notification
    }

    pub async fn set_user_secret_key(&self, access_key: &str, secret_key: &str) -> Result<()> {
        if !is_access_key_valid(access_key) {
            return Err(IamError::InvalidAccessKeyLength.into());
        }

        if !is_secret_key_valid(secret_key) {
            return Err(IamError::InvalidSecretKeyLength.into());
        }

        self.store.update_user_secret_key(access_key, secret_key).await
    }

    pub async fn check_key(&self, access_key: &str) -> Result<(Option<UserIdentity>, bool)> {
        if let Some(sys_cred) = get_global_action_cred() {
            if sys_cred.access_key == access_key {
                return Ok((Some(UserIdentity::new(sys_cred)), true));
            }
        }

        match self.store.get_user(access_key).await {
            Some(res) => {
                let ok = res.credentials.is_valid();

                Ok((Some(res), ok))
            }
            None => Ok((None, false)),
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
            return Err(IamError::GroupNameContainsReservedChars.into());
        }
        self.store.add_users_to_group(group, users).await
        // TODO: notification
    }

    pub async fn remove_users_from_group(&self, group: &str, users: Vec<String>) -> Result<OffsetDateTime> {
        self.store.remove_users_from_group(group, users).await
        // TODO: notification
    }

    pub async fn set_group_status(&self, group: &str, enable: bool) -> Result<OffsetDateTime> {
        self.store.set_group_status(group, enable).await
        // TODO: notification
    }
    pub async fn get_group_description(&self, group: &str) -> Result<GroupDesc> {
        self.store.get_group_description(group).await
    }

    pub async fn list_groups(&self) -> Result<Vec<String>> {
        self.store.list_groups().await
    }

    pub async fn policy_db_set(&self, name: &str, user_type: UserType, is_group: bool, policy: &str) -> Result<OffsetDateTime> {
        self.store.policy_db_set(name, user_type, is_group, policy).await
        // TODO: notification
    }

    pub async fn policy_db_get(&self, name: &str, groups: &[String]) -> Result<Vec<String>> {
        self.store.policy_db_get(name, groups).await
    }

    pub async fn is_allowed_sts(&self, args: &Args<'_>, parent_user: &str) -> bool {
        let is_owner = parent_user == get_global_action_cred().unwrap().access_key;
        let role_arn = args.get_role_arn();
        let policies = {
            if is_owner {
                Vec::new()
            } else if role_arn.is_some() {
                let Ok(arn) = ARN::parse(role_arn.unwrap_or_default()) else { return false };

                MappedPolicy::new(self.roles_map.get(&arn).map_or_else(String::default, |v| v.clone()).as_str()).to_slice()
            } else {
                let Ok(p) = self.policy_db_get(parent_user, args.groups).await else { return false };

                p
                //TODO: FROM JWT
            }
        };

        if policies.is_empty() {
            return false;
        }

        let combined_policy = {
            if is_owner {
                Policy::default()
            } else {
                let (a, c) = self.store.merge_policies(&policies.join(",")).await;
                if a.is_empty() {
                    return false;
                }
                c
            }
        };

        let (has_session_policy, is_allowed_sp) = is_allowed_by_session_policy(args);
        if has_session_policy {
            return is_allowed_sp && (is_owner || combined_policy.is_allowed(args));
        }

        is_owner || combined_policy.is_allowed(args)
    }

    pub async fn is_allowed_service_account(&self, args: &Args<'_>, parent_user: &str) -> bool {
        let Some(p) = args.claims.get("parent") else {
            return false;
        };

        if p.as_str() != Some(parent_user) {
            return false;
        }

        let is_owner = parent_user == get_global_action_cred().unwrap().access_key;

        let role_arn = args.get_role_arn();

        let svc_policies = {
            if is_owner {
                Vec::new()
            } else if role_arn.is_some() {
                let Ok(arn) = ARN::parse(role_arn.unwrap_or_default()) else { return false };
                MappedPolicy::new(self.roles_map.get(&arn).map_or_else(String::default, |v| v.clone()).as_str()).to_slice()
            } else {
                let Ok(p) = self.policy_db_get(parent_user, args.groups).await else { return false };
                p
            }
        };

        if !is_owner && svc_policies.is_empty() {
            return false;
        }

        let combined_policy = {
            if is_owner {
                Policy::default()
            } else {
                let (a, c) = self.store.merge_policies(&svc_policies.join(",")).await;
                if a.is_empty() {
                    return false;
                }
                c
            }
        };

        let mut parent_args = args.clone();
        parent_args.account = parent_user;

        let Some(sa) = args.claims.get(&iam_policy_claim_name_sa()) else {
            return false;
        };

        let Some(sa_str) = sa.as_str() else {
            return false;
        };

        if sa_str == INHERITED_POLICY_TYPE {
            return is_owner || combined_policy.is_allowed(&parent_args);
        }

        let (has_session_policy, is_allowed_sp) = is_allowed_by_session_policy_for_service_account(args);
        if has_session_policy {
            return is_allowed_sp && (is_owner || combined_policy.is_allowed(&parent_args));
        }

        is_owner || combined_policy.is_allowed(&parent_args)
    }

    async fn get_combined_policy(&self, policies: &[String]) -> Policy {
        self.store.merge_policies(&policies.join(",")).await.1
    }

    pub async fn is_allowed(&self, args: &Args<'_>) -> bool {
        if args.is_owner {
            return true;
        }

        let Ok((is_temp, parent_user)) = self.is_temp_user(args.account).await else { return false };

        if is_temp {
            return self.is_allowed_sts(args, &parent_user).await;
        }

        let Ok((is_svc, parent_user)) = self.is_service_account(args.account).await else { return false };

        if is_svc {
            return self.is_allowed_service_account(args, &parent_user).await;
        }

        let Ok(policies) = self.policy_db_get(args.account, args.groups).await else { return false };

        if policies.is_empty() {
            return false;
        }

        self.get_combined_policy(&policies).await.is_allowed(args)
    }
}

fn is_allowed_by_session_policy(args: &Args<'_>) -> (bool, bool) {
    let Some(spolicy) = args.claims.get(SESSION_POLICY_NAME_EXTRACTED) else {
        return (false, false);
    };

    let has_session_policy = true;

    let Some(spolicy_str) = spolicy.as_str() else {
        return (has_session_policy, false);
    };

    let Ok(sub_policy) = Policy::parse_config(spolicy_str.as_bytes()) else {
        return (has_session_policy, false);
    };

    if sub_policy.version.is_empty() {
        return (has_session_policy, false);
    }

    let mut session_policy_args = args.clone();
    session_policy_args.is_owner = false;

    (has_session_policy, sub_policy.is_allowed(&session_policy_args))
}

fn is_allowed_by_session_policy_for_service_account(args: &Args<'_>) -> (bool, bool) {
    let Some(spolicy) = args.claims.get(SESSION_POLICY_NAME_EXTRACTED) else {
        return (false, false);
    };

    let mut has_session_policy = true;

    let Some(spolicy_str) = spolicy.as_str() else {
        return (has_session_policy, false);
    };

    let Ok(sub_policy) = Policy::parse_config(spolicy_str.as_bytes()) else {
        return (has_session_policy, false);
    };

    if sub_policy.version.is_empty() && sub_policy.statements.is_empty() && sub_policy.id.is_empty() {
        has_session_policy = false;
        return (has_session_policy, false);
    }

    let mut session_policy_args = args.clone();
    session_policy_args.is_owner = false;

    (has_session_policy, sub_policy.is_allowed(&session_policy_args))
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
    pub claims: Option<HashMap<String, String>>,
}

pub struct UpdateServiceAccountOpts {
    pub session_policy: Option<Policy>,
    pub secret_key: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub expiration: Option<OffsetDateTime>,
    pub status: Option<String>,
}

pub fn iam_policy_claim_name_sa() -> String {
    "sa-policy".to_string()
}

/// DEFAULT_VERSION is the default version.
/// https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_version.html
pub const DEFAULT_VERSION: &str = "2012-10-17";

/// check the data is Validator
pub trait Validator {
    type Error;
    fn is_valid(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Args<'a> {
    pub account: &'a str,
    pub groups: &'a [String],
    pub action: Action,
    pub bucket: &'a str,
    pub conditions: &'a HashMap<String, Vec<String>>,
    pub is_owner: bool,
    pub object: &'a str,
    pub claims: &'a HashMap<String, Value>,
    pub deny_only: bool,
}

impl Args<'_> {
    pub fn get_role_arn(&self) -> Option<&str> {
        self.claims.get("roleArn").and_then(|x| x.as_str())
    }
    pub fn get_policies(&self, policy_claim_name: &str) -> (HashSet<String>, bool) {
        get_policies_from_claims(self.claims, policy_claim_name)
    }
}

fn get_values_from_claims(claims: &HashMap<String, Value>, claim_name: &str) -> (HashSet<String>, bool) {
    let mut s = HashSet::new();
    if let Some(pname) = claims.get(claim_name) {
        if let Some(pnames) = pname.as_array() {
            for pname in pnames {
                if let Some(pname_str) = pname.as_str() {
                    for pname in pname_str.split(',') {
                        let pname = pname.trim();
                        if !pname.is_empty() {
                            s.insert(pname.to_string());
                        }
                    }
                }
            }
            return (s, true);
        } else if let Some(pname_str) = pname.as_str() {
            for pname in pname_str.split(',') {
                let pname = pname.trim();
                if !pname.is_empty() {
                    s.insert(pname.to_string());
                }
            }
            return (s, true);
        }
    }
    (s, false)
}

fn get_policies_from_claims(claims: &HashMap<String, Value>, policy_claim_name: &str) -> (HashSet<String>, bool) {
    get_values_from_claims(claims, policy_claim_name)
}
