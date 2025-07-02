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

use http::HeaderMap;
use http::Uri;
use rustfs_ecstore::global::get_global_action_cred;
use rustfs_iam::error::Error as IamError;
use rustfs_iam::sys::SESSION_POLICY_NAME;
use rustfs_policy::auth;
use rustfs_policy::auth::get_claims_from_token_with_secret;
use s3s::S3Error;
use s3s::S3ErrorCode;
use s3s::S3Result;
use s3s::auth::S3Auth;
use s3s::auth::SecretKey;
use s3s::auth::SimpleAuth;
use s3s::s3_error;
use serde_json::Value;
use std::collections::HashMap;

pub struct IAMAuth {
    simple_auth: SimpleAuth,
}

impl IAMAuth {
    pub fn new(ak: impl Into<String>, sk: impl Into<SecretKey>) -> Self {
        let simple_auth = SimpleAuth::from_single(ak, sk);
        Self { simple_auth }
    }
}

#[async_trait::async_trait]
impl S3Auth for IAMAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        if access_key.is_empty() {
            return Err(s3_error!(UnauthorizedAccess, "Your account is not signed up"));
        }

        if let Ok(key) = self.simple_auth.get_secret_key(access_key).await {
            return Ok(key);
        }

        if let Ok(iam_store) = rustfs_iam::get() {
            if let Some(id) = iam_store.get_user(access_key).await {
                return Ok(SecretKey::from(id.credentials.secret_key.clone()));
            }
        }

        Err(s3_error!(UnauthorizedAccess, "Your account is not signed up2"))
    }
}

// check_key_valid checks the key is valid or not. return the user's credentials and if the user is the owner.
pub async fn check_key_valid(session_token: &str, access_key: &str) -> S3Result<(auth::Credentials, bool)> {
    let Some(mut cred) = get_global_action_cred() else {
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("get_global_action_cred {:?}", IamError::IamSysNotInitialized),
        ));
    };

    let sys_cred = cred.clone();

    if cred.access_key != access_key {
        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("check_key_valid {:?}", IamError::IamSysNotInitialized),
            ));
        };

        let (u, ok) = iam_store
            .check_key(access_key)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("check claims failed1 {e}")))?;

        if !ok {
            if let Some(u) = u {
                if u.credentials.status == "off" {
                    return Err(s3_error!(InvalidRequest, "ErrAccessKeyDisabled"));
                }
            }

            return Err(s3_error!(InvalidRequest, "ErrAccessKeyDisabled"));
        }

        let Some(u) = u else {
            return Err(s3_error!(InvalidRequest, "check key failed"));
        };

        cred = u.credentials;
    }

    let claims = check_claims_from_token(session_token, &cred)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("check claims failed {e}")))?;

    cred.claims = if !claims.is_empty() { Some(claims) } else { None };

    let mut owner = sys_cred.access_key == cred.access_key || cred.parent_user == sys_cred.access_key;

    // permitRootAccess
    if let Some(claims) = &cred.claims {
        if claims.contains_key(SESSION_POLICY_NAME) {
            owner = false
        }
    }

    Ok((cred, owner))
}

pub fn check_claims_from_token(token: &str, cred: &auth::Credentials) -> S3Result<HashMap<String, Value>> {
    if !token.is_empty() && cred.access_key.is_empty() {
        return Err(s3_error!(InvalidRequest, "no access key"));
    }

    if token.is_empty() && cred.is_temp() && !cred.is_service_account() {
        return Err(s3_error!(InvalidRequest, "invalid token1"));
    }

    if !token.is_empty() && !cred.is_temp() {
        return Err(s3_error!(InvalidRequest, "invalid token2"));
    }

    if !cred.is_service_account() && cred.is_temp() && token != cred.session_token {
        return Err(s3_error!(InvalidRequest, "invalid token3"));
    }

    if cred.is_temp() && cred.is_expired() {
        return Err(s3_error!(InvalidRequest, "invalid access key is temp and expired"));
    }

    let Some(sys_cred) = get_global_action_cred() else {
        return Err(s3_error!(InternalError, "action cred not init"));
    };

    // TODO: REPLICATION

    let (token, secret) = if cred.is_service_account() {
        (cred.session_token.as_str(), cred.secret_key.as_str())
    } else {
        (token, sys_cred.secret_key.as_str())
    };

    if !token.is_empty() {
        let claims: HashMap<String, Value> =
            get_claims_from_token_with_secret(token, secret).map_err(|_e| s3_error!(InvalidRequest, "invalid token"))?;
        return Ok(claims);
    }

    Ok(HashMap::new())
}

pub fn get_session_token<'a>(uri: &'a Uri, hds: &'a HeaderMap) -> Option<&'a str> {
    hds.get("x-amz-security-token")
        .map(|v| v.to_str().unwrap_or_default())
        .or_else(|| get_query_param(uri.query().unwrap_or_default(), "x-amz-security-token"))
}

pub fn get_condition_values(header: &HeaderMap, cred: &auth::Credentials) -> HashMap<String, Vec<String>> {
    let username = if cred.is_temp() || cred.is_service_account() {
        cred.parent_user.clone()
    } else {
        cred.access_key.clone()
    };

    let sys_cred = get_global_action_cred().unwrap_or_default();

    let claims = &cred.claims;

    let principal_type = if !username.is_empty() {
        if claims.is_some() {
            "AssumedRole"
        } else if sys_cred.access_key == username {
            "Account"
        } else {
            "User"
        }
    } else {
        "Anonymous"
    };

    let mut args = HashMap::new();
    args.insert("userid".to_owned(), vec![username.clone()]);
    args.insert("username".to_owned(), vec![username]);
    args.insert("principaltype".to_owned(), vec![principal_type.to_string()]);

    let mut clone_header = header.clone();
    if let Some(v) = clone_header.get("x-amz-signature-age") {
        args.insert("signatureAge".to_string(), vec![v.to_str().unwrap_or("").to_string()]);
        clone_header.remove("x-amz-signature-age");
    }

    // TODO: parse_object_tags
    // if let Some(_user_tags) = clone_header.get("x-amz-tagging") {
    // TODO: parse_object_tags
    // if let Ok(tag) = tags::parse_object_tags(user_tags.to_str().unwrap_or("")) {
    //     let tag_map = tag.to_map();
    //     let mut keys = Vec::new();
    //     for (k, v) in tag_map {
    //         args.insert(format!("ExistingObjectTag/{}", k), vec![v.clone()]);
    //         args.insert(format!("RequestObjectTag/{}", k), vec![v.clone()]);
    //         keys.push(k);
    //     }
    //     args.insert("RequestObjectTagKeys".to_string(), keys);
    // }
    // }

    for obj_lock in &[
        "x-amz-object-lock-mode",
        "x-amz-object-lock-legal-hold",
        "x-amz-object-lock-retain-until-date",
    ] {
        let values = clone_header
            .get_all(*obj_lock)
            .iter()
            .map(|v| v.to_str().unwrap_or("").to_string())
            .collect::<Vec<String>>();
        if !values.is_empty() {
            args.insert(obj_lock.trim_start_matches("x-amz-").to_string(), values);
        }
        clone_header.remove(*obj_lock);
    }

    for (key, _values) in clone_header.iter() {
        if key.as_str().eq_ignore_ascii_case("x-amz-tagging") {
            continue;
        }
        if let Some(existing_values) = args.get_mut(key.as_str()) {
            existing_values.extend(clone_header.get_all(key).iter().map(|v| v.to_str().unwrap_or("").to_string()));
        } else {
            args.insert(
                key.as_str().to_string(),
                header
                    .get_all(key)
                    .iter()
                    .map(|v| v.to_str().unwrap_or("").to_string())
                    .collect(),
            );
        }
    }

    // TODO: add from url query
    // let mut clone_url_values = r
    //     .uri()
    //     .query()
    //     .unwrap_or("")
    //     .split('&')
    //     .map(|s| {
    //         let mut split = s.split('=');
    //         (split.next().unwrap_or("").to_string(), split.next().unwrap_or("").to_string())
    //     })
    //     .collect::<HashMap<String, String>>();

    // for obj_lock in &[
    //     "x-amz-object-lock-mode",
    //     "x-amz-object-lock-legal-hold",
    //     "x-amz-object-lock-retain-until-date",
    // ] {
    //     if let Some(values) = clone_url_values.get(*obj_lock) {
    //         args.insert(obj_lock.trim_start_matches("x-amz-").to_string(), vec![values.clone()]);
    //     }
    //     clone_url_values.remove(*obj_lock);
    // }

    // for (key, values) in clone_url_values.iter() {
    //     if let Some(existing_values) = args.get_mut(key) {
    //         existing_values.push(values.clone());
    //     } else {
    //         args.insert(key.clone(), vec![values.clone()]);
    //     }
    // }

    if let Some(claims) = &cred.claims {
        for (k, v) in claims {
            if let Some(v_str) = v.as_str() {
                args.insert(k.trim_start_matches("ldap").to_lowercase(), vec![v_str.to_string()]);
            }
        }

        if let Some(grps_val) = claims.get("groups") {
            if let Some(grps_is) = grps_val.as_array() {
                let grps = grps_is
                    .iter()
                    .filter_map(|g| g.as_str().map(|s| s.to_string()))
                    .collect::<Vec<String>>();
                if !grps.is_empty() {
                    args.insert("groups".to_string(), grps);
                }
            }
        }
    }

    if let Some(groups) = &cred.groups {
        if !args.contains_key("groups") {
            args.insert("groups".to_string(), groups.clone());
        }
    }

    args
}

pub fn get_query_param<'a>(query: &'a str, param_name: &str) -> Option<&'a str> {
    let param_name = param_name.to_lowercase();

    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
            if key.to_lowercase() == param_name {
                return Some(value);
            }
        }
    }
    None
}
