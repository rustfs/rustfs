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

pub(crate) async fn resolve_info_access_key_resp<T: IamStore>(
    iam_store: &rustfs_iam::sys::IamSys<T>,
    access_key: String,
    target_cred: StoredCredentials,
) -> S3Result<InfoAccessKeyResp> {
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
            "STS".to_string(),
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
            "Service Account".to_string(),
            build_info_service_account_resp(iam_store, &target_cred, session_policy).await?,
        )
    } else {
        let user_info = iam_store.get_user_info(&access_key).await.map_err(|e| {
            debug!("get user info failed, e: {:?}", e);
            iam_error_to_s3_error(e)
        })?;
        ("User".to_string(), build_info_regular_user_resp(&target_cred, &user_info))
    };

    let user_provider = guess_user_provider(&target_cred).to_string();
    Ok(InfoAccessKeyResp {
        access_key,
        info,
        user_type,
        user_provider: user_provider.clone(),
        ldap_specific_info: if user_provider == "ldap" {
            ldap_specific_info(target_cred.claims.as_ref())
        } else {
            LDAPSpecificAccessKeyInfo::default()
        },
        open_id_specific_info: if user_provider == "openid" {
            openid_specific_info(target_cred.claims.as_ref())
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

pub(crate) fn guess_user_provider(credentials: &StoredCredentials) -> &'static str {
    if !credentials.is_service_account() && !credentials.is_temp() {
        return "builtin";
    }

    let Some(claims) = credentials.claims.as_ref() else {
        return "builtin";
    };

    if claims.contains_key("ldap:user") || claims.contains_key("ldap:username") {
        return "ldap";
    }

    if claims.contains_key("sub") {
        return "openid";
    }

    "builtin"
}

pub(crate) fn ldap_specific_info(claims: Option<&HashMap<String, serde_json::Value>>) -> LDAPSpecificAccessKeyInfo {
    let username = claims
        .and_then(|claims| {
            claims
                .get("ldap:user")
                .or_else(|| claims.get("ldap:username"))
                .and_then(|value| value.as_str())
        })
        .map(ToOwned::to_owned);

    LDAPSpecificAccessKeyInfo { username }
}

pub(crate) fn openid_specific_info(claims: Option<&HashMap<String, serde_json::Value>>) -> OpenIDSpecificAccessKeyInfo {
    let user_id = claims
        .and_then(|claims| claims.get("sub"))
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned);
    let display_name = claims
        .and_then(|claims| claims.get("name"))
        .and_then(|value| value.as_str())
        .map(ToOwned::to_owned);

    OpenIDSpecificAccessKeyInfo {
        config_name: None,
        user_id: user_id.clone(),
        user_id_claim: user_id.as_ref().map(|_| "sub".to_string()),
        display_name: display_name.clone(),
        display_name_claim: display_name.as_ref().map(|_| "name".to_string()),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_iam::error::Error as IamError;

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
}
