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

use crate::admin::handlers::iam_error::iam_error_to_s3_error;
use rustfs_credentials::Credentials as StoredCredentials;
use rustfs_iam::error::{is_err_no_such_service_account, is_err_no_such_temp_account};
use rustfs_iam::store::Store as IamStore;
use rustfs_madmin::{
    InfoAccessKeyResp, InfoServiceAccountResp, LDAPSpecificAccessKeyInfo, OpenIDSpecificAccessKeyInfo, ServiceAccountInfo,
};
use rustfs_policy::policy::Policy;
use s3s::{S3Result, s3_error};
use std::collections::HashMap;
use time::OffsetDateTime;
use tracing::debug;

#[derive(Debug, Clone, PartialEq, Eq)]
enum AccessKeyUserType {
    User,
    ServiceAccount,
    Sts,
}

impl AccessKeyUserType {
    fn as_str(&self) -> &'static str {
        match self {
            Self::User => "User",
            Self::ServiceAccount => "Service Account",
            Self::Sts => "STS",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AccessKeyProvider {
    Builtin,
    Ldap,
    OpenId,
}

impl AccessKeyProvider {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Builtin => "builtin",
            Self::Ldap => "ldap",
            Self::OpenId => "openid",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum StableSubjectHint {
    BuiltinAccessKey(String),
    LdapUser(String),
    OpenId { issuer: String, subject: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct AccessKeyIdentity {
    user_type: AccessKeyUserType,
    provider: AccessKeyProvider,
    stable_subject_hint: Option<StableSubjectHint>,
}

impl AccessKeyIdentity {
    fn from_credentials(credentials: &StoredCredentials) -> Self {
        let user_type = if credentials.is_temp() {
            AccessKeyUserType::Sts
        } else if credentials.is_service_account() {
            AccessKeyUserType::ServiceAccount
        } else {
            AccessKeyUserType::User
        };

        let provider = match user_type {
            AccessKeyUserType::User => AccessKeyProvider::Builtin,
            AccessKeyUserType::ServiceAccount | AccessKeyUserType::Sts => {
                classify_provider_from_claims(credentials.claims.as_ref())
            }
        };

        let stable_subject_hint = match provider {
            AccessKeyProvider::Builtin => match user_type {
                AccessKeyUserType::User => Some(StableSubjectHint::BuiltinAccessKey(credentials.access_key.clone())),
                AccessKeyUserType::ServiceAccount | AccessKeyUserType::Sts => None,
            },
            AccessKeyProvider::Ldap => {
                claim_string(credentials.claims.as_ref(), &["ldap:user", "ldap:username"]).map(StableSubjectHint::LdapUser)
            }
            AccessKeyProvider::OpenId => stable_openid_subject_hint(credentials.claims.as_ref()),
        };

        Self {
            user_type,
            provider,
            stable_subject_hint,
        }
    }

    fn ldap_specific_info(&self, claims: Option<&HashMap<String, serde_json::Value>>) -> LDAPSpecificAccessKeyInfo {
        let username = match &self.stable_subject_hint {
            Some(StableSubjectHint::LdapUser(username)) => Some(username.clone()),
            _ => claim_string(claims, &["ldap:user", "ldap:username"]),
        };

        LDAPSpecificAccessKeyInfo { username }
    }

    fn openid_specific_info(&self, claims: Option<&HashMap<String, serde_json::Value>>) -> OpenIDSpecificAccessKeyInfo {
        let user_id = match &self.stable_subject_hint {
            Some(StableSubjectHint::OpenId { subject, .. }) => Some(subject.clone()),
            _ => claim_string(claims, &["sub"]),
        };
        let display_name = claim_string(claims, &["name"]);

        OpenIDSpecificAccessKeyInfo {
            config_name: None,
            user_id: user_id.clone(),
            user_id_claim: user_id.as_ref().map(|_| "sub".to_string()),
            display_name: display_name.clone(),
            display_name_claim: display_name.as_ref().map(|_| "name".to_string()),
        }
    }
}

fn identity_from_provider(
    user_type: AccessKeyUserType,
    provider: AccessKeyProvider,
    claims: Option<&HashMap<String, serde_json::Value>>,
) -> AccessKeyIdentity {
    let stable_subject_hint = match provider {
        AccessKeyProvider::Builtin => None,
        AccessKeyProvider::Ldap => claim_string(claims, &["ldap:user", "ldap:username"]).map(StableSubjectHint::LdapUser),
        AccessKeyProvider::OpenId => stable_openid_subject_hint(claims),
    };

    AccessKeyIdentity {
        user_type,
        provider,
        stable_subject_hint,
    }
}

pub(crate) async fn resolve_info_access_key_resp<T: IamStore>(
    iam_store: &rustfs_iam::sys::IamSys<T>,
    access_key: String,
    target_cred: StoredCredentials,
) -> S3Result<InfoAccessKeyResp> {
    let identity = AccessKeyIdentity::from_credentials(&target_cred);
    let (user_type, info) = if target_cred.is_temp() {
        let (_, session_policy) = iam_store.get_temporary_account(&access_key).await.map_err(|e| {
            debug!("get temporary account failed, e: {:?}", e);
            if is_err_no_such_temp_account(&e) {
                s3_error!(InvalidRequest, "access key not exist")
            } else {
                s3_error!(InternalError, "get temporary account failed")
            }
        })?;
        (
            identity.user_type.as_str().to_string(),
            build_info_service_account_resp(iam_store, &target_cred, session_policy).await?,
        )
    } else if target_cred.is_service_account() {
        let (_, session_policy) = iam_store.get_service_account(&access_key).await.map_err(|e| {
            debug!("get service account failed, e: {:?}", e);
            if is_err_no_such_service_account(&e) {
                s3_error!(InvalidRequest, "access key not exist")
            } else {
                s3_error!(InternalError, "get service account failed")
            }
        })?;
        (
            identity.user_type.as_str().to_string(),
            build_info_service_account_resp(iam_store, &target_cred, session_policy).await?,
        )
    } else {
        let user_info = iam_store.get_user_info(&access_key).await.map_err(|e| {
            debug!("get user info failed, e: {:?}", e);
            iam_error_to_s3_error(e)
        })?;
        (
            identity.user_type.as_str().to_string(),
            build_info_regular_user_resp(&target_cred, &user_info),
        )
    };

    let user_provider = identity.provider.as_str().to_string();
    Ok(InfoAccessKeyResp {
        access_key,
        info,
        user_type,
        user_provider: user_provider.clone(),
        ldap_specific_info: if user_provider == "ldap" {
            identity.ldap_specific_info(target_cred.claims.as_ref())
        } else {
            LDAPSpecificAccessKeyInfo::default()
        },
        open_id_specific_info: if user_provider == "openid" {
            identity.openid_specific_info(target_cred.claims.as_ref())
        } else {
            OpenIDSpecificAccessKeyInfo::default()
        },
    })
}

pub(crate) async fn build_info_service_account_resp<T: IamStore>(
    iam_store: &rustfs_iam::sys::IamSys<T>,
    account: &StoredCredentials,
    session_policy: Option<Policy>,
) -> S3Result<InfoServiceAccountResp> {
    let implied_policy = session_policy
        .as_ref()
        .is_none_or(|policy| policy.version.is_empty() && policy.statements.is_empty());

    let effective_policy = if implied_policy {
        let policies = iam_store
            .policy_db_get(&account.parent_user, &account.groups)
            .await
            .map_err(|e| {
                debug!("get service account policy failed, e: {:?}", e);
                s3_error!(InternalError, "get service account policy failed")
            })?;

        Some(iam_store.get_combined_policy(&policies).await)
    } else {
        session_policy
    };

    let policy = effective_policy
        .map(|policy| {
            serde_json::to_string_pretty(&policy).map_err(|e| {
                debug!("marshal policy failed, e: {:?}", e);
                s3_error!(InternalError, "marshal policy failed")
            })
        })
        .transpose()?;

    Ok(InfoServiceAccountResp {
        parent_user: account.parent_user.clone(),
        account_status: account.status.clone(),
        implied_policy,
        name: account.name.clone(),
        description: account.description.clone(),
        expiration: account.expiration,
        policy,
    })
}

pub(crate) fn build_info_regular_user_resp(
    account: &StoredCredentials,
    user_info: &rustfs_madmin::UserInfo,
) -> InfoServiceAccountResp {
    InfoServiceAccountResp {
        parent_user: String::new(),
        account_status: user_info.status.as_ref().to_string(),
        implied_policy: false,
        policy: user_info.policy_name.clone(),
        name: account.name.clone(),
        description: account.description.clone(),
        expiration: None,
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn guess_user_provider(credentials: &StoredCredentials) -> &'static str {
    AccessKeyIdentity::from_credentials(credentials).provider.as_str()
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn ldap_specific_info(claims: Option<&HashMap<String, serde_json::Value>>) -> LDAPSpecificAccessKeyInfo {
    identity_from_provider(AccessKeyUserType::ServiceAccount, AccessKeyProvider::Ldap, claims).ldap_specific_info(claims)
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn openid_specific_info(claims: Option<&HashMap<String, serde_json::Value>>) -> OpenIDSpecificAccessKeyInfo {
    identity_from_provider(AccessKeyUserType::ServiceAccount, AccessKeyProvider::OpenId, claims).openid_specific_info(claims)
}

pub(crate) fn list_entry_from_credentials(account: &StoredCredentials, expiration: Option<OffsetDateTime>) -> ServiceAccountInfo {
    ServiceAccountInfo {
        parent_user: String::new(),
        account_status: String::new(),
        implied_policy: false,
        access_key: account.access_key.clone(),
        name: account.name.clone(),
        description: account.description.clone(),
        expiration,
    }
}

fn classify_provider_from_claims(claims: Option<&HashMap<String, serde_json::Value>>) -> AccessKeyProvider {
    if claim_string(claims, &["ldap:user", "ldap:username"]).is_some() {
        return AccessKeyProvider::Ldap;
    }

    if claim_string(claims, &["sub"]).is_some() {
        return AccessKeyProvider::OpenId;
    }

    AccessKeyProvider::Builtin
}

fn stable_openid_subject_hint(claims: Option<&HashMap<String, serde_json::Value>>) -> Option<StableSubjectHint> {
    let issuer = claim_string(claims, &["iss"])?;
    let subject = claim_string(claims, &["sub"])?;

    Some(StableSubjectHint::OpenId { issuer, subject })
}

fn claim_string(claims: Option<&HashMap<String, serde_json::Value>>, keys: &[&str]) -> Option<String> {
    claims.and_then(|claims| {
        keys.iter()
            .find_map(|key| claims.get(*key).and_then(|value| value.as_str()).map(ToOwned::to_owned))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_iam::cache::Cache;
    use rustfs_iam::error::Error as IamError;
    use rustfs_iam::manager::IamCache;
    use rustfs_iam::store::{MappedPolicy, Store, UserType};
    use rustfs_iam::sys::IamSys;
    use rustfs_policy::auth::UserIdentity;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicU64};
    use tokio::sync::mpsc;

    #[derive(Clone)]
    struct InfoAccessKeyTestStore;

    #[async_trait::async_trait]
    impl Store for InfoAccessKeyTestStore {
        fn has_watcher(&self) -> bool {
            false
        }

        async fn save_iam_config<Item: serde::Serialize + Send>(
            &self,
            _item: Item,
            _path: impl AsRef<str> + Send,
        ) -> rustfs_iam::error::Result<()> {
            Ok(())
        }

        async fn load_iam_config<Item: serde::de::DeserializeOwned>(
            &self,
            _path: impl AsRef<str> + Send,
        ) -> rustfs_iam::error::Result<Item> {
            Err(IamError::ConfigNotFound)
        }

        async fn delete_iam_config(&self, _path: impl AsRef<str> + Send) -> rustfs_iam::error::Result<()> {
            Err(IamError::InvalidArgument)
        }

        async fn save_user_identity(
            &self,
            _name: &str,
            _user_type: UserType,
            _item: UserIdentity,
            _ttl: Option<usize>,
        ) -> rustfs_iam::error::Result<()> {
            Ok(())
        }

        async fn delete_user_identity(&self, _name: &str, _user_type: UserType) -> rustfs_iam::error::Result<()> {
            Err(IamError::InvalidArgument)
        }

        async fn load_user_identity(&self, _name: &str, _user_type: UserType) -> rustfs_iam::error::Result<UserIdentity> {
            Err(IamError::InvalidArgument)
        }

        async fn load_user(
            &self,
            _name: &str,
            _user_type: UserType,
            _m: &mut HashMap<String, UserIdentity>,
        ) -> rustfs_iam::error::Result<()> {
            Ok(())
        }

        async fn load_users(
            &self,
            _user_type: UserType,
            _m: &mut HashMap<String, UserIdentity>,
        ) -> rustfs_iam::error::Result<()> {
            Ok(())
        }

        async fn load_secret_key(&self, _name: &str, _user_type: UserType) -> rustfs_iam::error::Result<String> {
            Err(IamError::InvalidArgument)
        }

        async fn save_group_info(&self, _name: &str, _item: rustfs_iam::store::GroupInfo) -> rustfs_iam::error::Result<()> {
            Err(IamError::InvalidArgument)
        }

        async fn delete_group_info(&self, _name: &str) -> rustfs_iam::error::Result<()> {
            Err(IamError::InvalidArgument)
        }

        async fn load_group(
            &self,
            _name: &str,
            _m: &mut HashMap<String, rustfs_iam::store::GroupInfo>,
        ) -> rustfs_iam::error::Result<()> {
            Ok(())
        }

        async fn load_groups(&self, _m: &mut HashMap<String, rustfs_iam::store::GroupInfo>) -> rustfs_iam::error::Result<()> {
            Ok(())
        }

        async fn save_policy_doc(&self, _name: &str, _item: rustfs_policy::policy::PolicyDoc) -> rustfs_iam::error::Result<()> {
            Err(IamError::InvalidArgument)
        }

        async fn delete_policy_doc(&self, _name: &str) -> rustfs_iam::error::Result<()> {
            Err(IamError::InvalidArgument)
        }

        async fn load_policy(&self, _name: &str) -> rustfs_iam::error::Result<rustfs_policy::policy::PolicyDoc> {
            Err(IamError::InvalidArgument)
        }

        async fn load_policy_doc(
            &self,
            _name: &str,
            _m: &mut HashMap<String, rustfs_policy::policy::PolicyDoc>,
        ) -> rustfs_iam::error::Result<()> {
            Ok(())
        }

        async fn load_policy_docs(
            &self,
            _m: &mut HashMap<String, rustfs_policy::policy::PolicyDoc>,
        ) -> rustfs_iam::error::Result<()> {
            Ok(())
        }

        async fn save_mapped_policy(
            &self,
            _name: &str,
            _user_type: UserType,
            _is_group: bool,
            _item: MappedPolicy,
            _ttl: Option<usize>,
        ) -> rustfs_iam::error::Result<()> {
            Ok(())
        }

        async fn delete_mapped_policy(
            &self,
            _name: &str,
            _user_type: UserType,
            _is_group: bool,
        ) -> rustfs_iam::error::Result<()> {
            Err(IamError::InvalidArgument)
        }

        async fn load_mapped_policy(
            &self,
            _name: &str,
            _user_type: UserType,
            _is_group: bool,
            _m: &mut HashMap<String, MappedPolicy>,
        ) -> rustfs_iam::error::Result<()> {
            Ok(())
        }

        async fn load_mapped_policies(
            &self,
            _user_type: UserType,
            _is_group: bool,
            _m: &mut HashMap<String, MappedPolicy>,
        ) -> rustfs_iam::error::Result<()> {
            Ok(())
        }

        async fn load_all(&self, _cache: &Cache) -> rustfs_iam::error::Result<()> {
            Ok(())
        }
    }

    fn test_iam_sys_with_user(
        access_key: &str,
        status: rustfs_madmin::AccountStatus,
        policy_name: Option<&str>,
    ) -> IamSys<InfoAccessKeyTestStore> {
        let (sender, _receiver) = mpsc::channel::<i64>(1);
        let cache = Cache::default();
        let now = OffsetDateTime::now_utc();
        cache.add_or_update_user(
            access_key,
            &UserIdentity::from(StoredCredentials {
                access_key: access_key.to_string(),
                secret_key: "secret-key".to_string(),
                status: match status {
                    rustfs_madmin::AccountStatus::Enabled => "on".to_string(),
                    rustfs_madmin::AccountStatus::Disabled => "off".to_string(),
                },
                ..Default::default()
            }),
            now,
        );
        if let Some(policy_name) = policy_name {
            cache.add_or_update_user_policy(access_key, &MappedPolicy::new(policy_name), now);
        }

        IamSys::new(Arc::new(IamCache {
            api: InfoAccessKeyTestStore,
            cache,
            state: Arc::new(AtomicU8::new(2)),
            loading: Arc::new(AtomicBool::new(false)),
            roles: HashMap::new(),
            send_chan: sender,
            last_timestamp: AtomicI64::new(now.unix_timestamp()),
            sync_failures: AtomicU64::new(0),
            sync_successes: AtomicU64::new(0),
            last_sync_duration_millis: AtomicU64::new(0),
        }))
    }

    #[test]
    fn regular_user_lookup_errors_match_shared_iam_mapper() {
        let mapped = {
            let err = IamError::NoSuchUser("missing-user".to_string());
            debug!("get user info failed, e: {:?}", err);
            iam_error_to_s3_error(err)
        };
        let expected = iam_error_to_s3_error(IamError::NoSuchUser("missing-user".to_string()));

        assert_eq!(mapped.code(), expected.code());
        assert_eq!(mapped.message(), expected.message());
        assert_eq!(mapped.source().is_some(), expected.source().is_some());
    }

    #[test]
    fn builtin_regular_user_identity_uses_access_key_as_stable_hint() {
        let credentials = StoredCredentials {
            access_key: "builtin-user".to_string(),
            ..Default::default()
        };

        let identity = AccessKeyIdentity::from_credentials(&credentials);

        assert_eq!(identity.user_type, AccessKeyUserType::User);
        assert_eq!(identity.provider, AccessKeyProvider::Builtin);
        assert_eq!(
            identity.stable_subject_hint,
            Some(StableSubjectHint::BuiltinAccessKey("builtin-user".to_string()))
        );
    }

    #[test]
    fn builtin_regular_user_takes_precedence_over_ldap_like_claims() {
        let credentials = StoredCredentials {
            access_key: "svc".to_string(),
            parent_user: "parent".to_string(),
            claims: Some(HashMap::from([("ldap:username".to_string(), json!("alice"))])),
            ..Default::default()
        };

        let identity = AccessKeyIdentity::from_credentials(&credentials);

        assert_eq!(identity.user_type, AccessKeyUserType::User);
        assert_eq!(identity.provider, AccessKeyProvider::Builtin);
        assert_eq!(identity.stable_subject_hint, Some(StableSubjectHint::BuiltinAccessKey("svc".to_string())));
    }

    #[test]
    fn ldap_service_account_identity_uses_known_ldap_claim_as_stable_hint() {
        let credentials = StoredCredentials {
            access_key: "svc".to_string(),
            parent_user: "parent".to_string(),
            claims: Some(HashMap::from([
                ("ldap:username".to_string(), json!("alice")),
                (rustfs_credentials::IAM_POLICY_CLAIM_NAME_SA.to_string(), json!("embedded")),
            ])),
            ..Default::default()
        };

        let identity = AccessKeyIdentity::from_credentials(&credentials);

        assert_eq!(identity.user_type, AccessKeyUserType::ServiceAccount);
        assert_eq!(identity.provider, AccessKeyProvider::Ldap);
        assert_eq!(identity.stable_subject_hint, Some(StableSubjectHint::LdapUser("alice".to_string())));
    }

    #[test]
    fn openid_identity_requires_issuer_for_stable_hint_but_keeps_legacy_user_id_projection() {
        let claims = HashMap::from([
            ("sub".to_string(), json!("subject-123")),
            ("name".to_string(), json!("RustFS User")),
        ]);
        let credentials = StoredCredentials {
            access_key: "sts".to_string(),
            session_token: "session-token".to_string(),
            parent_user: "parent".to_string(),
            claims: Some(claims.clone()),
            ..Default::default()
        };

        let identity = AccessKeyIdentity::from_credentials(&credentials);
        let openid_info = identity.openid_specific_info(Some(&claims));

        assert_eq!(identity.user_type, AccessKeyUserType::Sts);
        assert_eq!(identity.provider, AccessKeyProvider::OpenId);
        assert_eq!(identity.stable_subject_hint, None);
        assert_eq!(openid_info.user_id.as_deref(), Some("subject-123"));
        assert_eq!(openid_info.display_name.as_deref(), Some("RustFS User"));
    }

    #[test]
    fn openid_identity_uses_issuer_and_subject_as_stable_hint() {
        let credentials = StoredCredentials {
            access_key: "sts".to_string(),
            session_token: "session-token".to_string(),
            parent_user: "parent".to_string(),
            claims: Some(HashMap::from([
                ("iss".to_string(), json!("https://issuer.example/realms/rustfs")),
                ("sub".to_string(), json!("subject-123")),
            ])),
            ..Default::default()
        };

        let identity = AccessKeyIdentity::from_credentials(&credentials);

        assert_eq!(
            identity.stable_subject_hint,
            Some(StableSubjectHint::OpenId {
                issuer: "https://issuer.example/realms/rustfs".to_string(),
                subject: "subject-123".to_string(),
            })
        );
    }

    #[tokio::test]
    async fn resolve_info_access_key_resp_preserves_regular_user_contract() {
        let iam_sys = test_iam_sys_with_user("builtin-user", rustfs_madmin::AccountStatus::Enabled, Some("consoleAdmin"));
        let credentials = StoredCredentials {
            access_key: "builtin-user".to_string(),
            name: Some("Builtin User".to_string()),
            description: Some("regular user".to_string()),
            ..Default::default()
        };

        let resp = resolve_info_access_key_resp(&iam_sys, "builtin-user".to_string(), credentials)
            .await
            .expect("resolve info access key");

        assert_eq!(resp.user_type, "User");
        assert_eq!(resp.user_provider, "builtin");
        assert_eq!(resp.info.account_status, "enabled");
        assert_eq!(resp.info.policy.as_deref(), Some("consoleAdmin"));
        assert_eq!(resp.info.name.as_deref(), Some("Builtin User"));
        assert_eq!(resp.info.description.as_deref(), Some("regular user"));
        assert_eq!(resp.ldap_specific_info.username, None);
        assert_eq!(resp.open_id_specific_info.user_id, None);
    }
}
