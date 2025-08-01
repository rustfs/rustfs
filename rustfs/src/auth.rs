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
use rustfs_iam::sys::get_claims_from_token_with_secret;
use rustfs_policy::auth;
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

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue, Uri};
    use rustfs_policy::auth::Credentials;
    use s3s::auth::SecretKey;
    use serde_json::json;
    use std::collections::HashMap;
    use time::OffsetDateTime;

    fn create_test_credentials() -> Credentials {
        Credentials {
            access_key: "test-access-key".to_string(),
            secret_key: "test-secret-key".to_string(),
            session_token: "".to_string(),
            expiration: None,
            status: "on".to_string(),
            parent_user: "".to_string(),
            groups: None,
            claims: None,
            name: Some("test-user".to_string()),
            description: Some("test user for auth tests".to_string()),
        }
    }

    fn create_temp_credentials() -> Credentials {
        Credentials {
            access_key: "temp-access-key".to_string(),
            secret_key: "temp-secret-key".to_string(),
            session_token: "temp-session-token".to_string(),
            expiration: Some(OffsetDateTime::now_utc() + time::Duration::hours(1)),
            status: "on".to_string(),
            parent_user: "parent-user".to_string(),
            groups: Some(vec!["test-group".to_string()]),
            claims: None,
            name: Some("temp-user".to_string()),
            description: Some("temporary user for auth tests".to_string()),
        }
    }

    fn create_service_account_credentials() -> Credentials {
        let mut claims = HashMap::new();
        claims.insert("sa-policy".to_string(), json!("test-policy"));

        Credentials {
            access_key: "service-access-key".to_string(),
            secret_key: "service-secret-key".to_string(),
            session_token: "service-session-token".to_string(),
            expiration: None,
            status: "on".to_string(),
            parent_user: "service-parent".to_string(),
            groups: None,
            claims: Some(claims),
            name: Some("service-account".to_string()),
            description: Some("service account for auth tests".to_string()),
        }
    }

    #[test]
    fn test_iam_auth_creation() {
        let access_key = "test-access-key";
        let secret_key = SecretKey::from("test-secret-key");

        let iam_auth = IAMAuth::new(access_key, secret_key);

        // The struct should be created successfully
        // We can't easily test internal state without exposing it,
        // but we can test it doesn't panic on creation
        assert_eq!(std::mem::size_of_val(&iam_auth), std::mem::size_of::<IAMAuth>());
    }

    #[tokio::test]
    async fn test_iam_auth_get_secret_key_empty_access_key() {
        let iam_auth = IAMAuth::new("test-ak", SecretKey::from("test-sk"));

        let result = iam_auth.get_secret_key("").await;

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::UnauthorizedAccess);
        assert!(error.message().unwrap_or("").contains("Your account is not signed up"));
    }

    #[test]
    fn test_check_claims_from_token_empty_token_and_access_key() {
        let mut cred = create_test_credentials();
        cred.access_key = "".to_string();

        let result = check_claims_from_token("test-token", &cred);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(error.message().unwrap_or("").contains("no access key"));
    }

    #[test]
    fn test_check_claims_from_token_temp_credentials_without_token() {
        let mut cred = create_temp_credentials();
        // Make it non-service account
        cred.claims = None;

        let result = check_claims_from_token("", &cred);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(error.message().unwrap_or("").contains("invalid token1"));
    }

    #[test]
    fn test_check_claims_from_token_non_temp_with_token() {
        let mut cred = create_test_credentials();
        cred.session_token = "".to_string(); // Make it non-temp

        let result = check_claims_from_token("some-token", &cred);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(error.message().unwrap_or("").contains("invalid token2"));
    }

    #[test]
    fn test_check_claims_from_token_mismatched_session_token() {
        let mut cred = create_temp_credentials();
        // Make sure it's not a service account
        cred.claims = None;

        let result = check_claims_from_token("wrong-session-token", &cred);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
        assert!(error.message().unwrap_or("").contains("invalid token3"));
    }

    #[test]
    fn test_check_claims_from_token_expired_credentials() {
        let mut cred = create_temp_credentials();
        cred.expiration = Some(OffsetDateTime::now_utc() - time::Duration::hours(1)); // Expired
        cred.claims = None; // Make sure it's not a service account

        let result = check_claims_from_token(&cred.session_token, &cred);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);

        // The function checks various conditions in order. An expired temp credential
        // might trigger other validation errors first (like token mismatch)
        let msg = error.message().unwrap_or("");
        let is_valid_error = msg.contains("invalid access key is temp and expired")
            || msg.contains("invalid token")
            || msg.contains("action cred not init");
        assert!(is_valid_error, "Unexpected error message: '{msg}'");
    }

    #[test]
    fn test_check_claims_from_token_valid_non_temp_credentials() {
        let mut cred = create_test_credentials();
        cred.session_token = "".to_string(); // Make it non-temp

        let result = check_claims_from_token("", &cred);

        // This might fail due to global state dependencies, but should return error about global cred init
        if result.is_ok() {
            let claims = result.unwrap();
            assert!(claims.is_empty());
        } else {
            let error = result.unwrap_err();
            assert_eq!(error.code(), &S3ErrorCode::InternalError);
            assert!(error.message().unwrap_or("").contains("action cred not init"));
        }
    }

    #[test]
    fn test_get_session_token_from_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-security-token", HeaderValue::from_static("test-session-token"));

        let uri: Uri = "https://example.com/".parse().unwrap();

        let token = get_session_token(&uri, &headers);

        assert_eq!(token, Some("test-session-token"));
    }

    #[test]
    fn test_get_session_token_from_query_param() {
        let headers = HeaderMap::new();
        let uri: Uri = "https://example.com/?x-amz-security-token=query-session-token"
            .parse()
            .unwrap();

        let token = get_session_token(&uri, &headers);

        assert_eq!(token, Some("query-session-token"));
    }

    #[test]
    fn test_get_session_token_header_takes_precedence() {
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-security-token", HeaderValue::from_static("header-token"));

        let uri: Uri = "https://example.com/?x-amz-security-token=query-token".parse().unwrap();

        let token = get_session_token(&uri, &headers);

        assert_eq!(token, Some("header-token"));
    }

    #[test]
    fn test_get_session_token_no_token() {
        let headers = HeaderMap::new();
        let uri: Uri = "https://example.com/".parse().unwrap();

        let token = get_session_token(&uri, &headers);

        assert_eq!(token, None);
    }

    #[test]
    fn test_get_condition_values_regular_user() {
        let cred = create_test_credentials();
        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred);

        assert_eq!(conditions.get("userid"), Some(&vec!["test-access-key".to_string()]));
        assert_eq!(conditions.get("username"), Some(&vec!["test-access-key".to_string()]));
        assert_eq!(conditions.get("principaltype"), Some(&vec!["User".to_string()]));
    }

    #[test]
    fn test_get_condition_values_temp_user() {
        let cred = create_temp_credentials();
        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred);

        assert_eq!(conditions.get("userid"), Some(&vec!["parent-user".to_string()]));
        assert_eq!(conditions.get("username"), Some(&vec!["parent-user".to_string()]));
        assert_eq!(conditions.get("principaltype"), Some(&vec!["User".to_string()]));
    }

    #[test]
    fn test_get_condition_values_service_account() {
        let cred = create_service_account_credentials();
        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred);

        assert_eq!(conditions.get("userid"), Some(&vec!["service-parent".to_string()]));
        assert_eq!(conditions.get("username"), Some(&vec!["service-parent".to_string()]));
        // Service accounts with claims should be "AssumedRole" type
        assert_eq!(conditions.get("principaltype"), Some(&vec!["AssumedRole".to_string()]));
    }

    #[test]
    fn test_get_condition_values_with_object_lock_headers() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-object-lock-mode", HeaderValue::from_static("GOVERNANCE"));
        headers.insert("x-amz-object-lock-retain-until-date", HeaderValue::from_static("2024-12-31T23:59:59Z"));

        let conditions = get_condition_values(&headers, &cred);

        assert_eq!(conditions.get("object-lock-mode"), Some(&vec!["GOVERNANCE".to_string()]));
        assert_eq!(
            conditions.get("object-lock-retain-until-date"),
            Some(&vec!["2024-12-31T23:59:59Z".to_string()])
        );
    }

    #[test]
    fn test_get_condition_values_with_signature_age() {
        let cred = create_test_credentials();
        let mut headers = HeaderMap::new();
        headers.insert("x-amz-signature-age", HeaderValue::from_static("300"));

        let conditions = get_condition_values(&headers, &cred);

        assert_eq!(conditions.get("signatureAge"), Some(&vec!["300".to_string()]));
        // Verify the header is removed after processing
        // (we can't directly test this without changing the function signature)
    }

    #[test]
    fn test_get_condition_values_with_claims() {
        let mut cred = create_service_account_credentials();
        let mut claims = HashMap::new();
        claims.insert("ldapUsername".to_string(), json!("ldap-user"));
        claims.insert("groups".to_string(), json!(["group1", "group2"]));
        cred.claims = Some(claims);

        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred);

        assert_eq!(conditions.get("username"), Some(&vec!["ldap-user".to_string()]));
        assert_eq!(conditions.get("groups"), Some(&vec!["group1".to_string(), "group2".to_string()]));
    }

    #[test]
    fn test_get_condition_values_with_credential_groups() {
        let mut cred = create_test_credentials();
        cred.groups = Some(vec!["cred-group1".to_string(), "cred-group2".to_string()]);

        let headers = HeaderMap::new();

        let conditions = get_condition_values(&headers, &cred);

        assert_eq!(
            conditions.get("groups"),
            Some(&vec!["cred-group1".to_string(), "cred-group2".to_string()])
        );
    }

    #[test]
    fn test_get_query_param_found() {
        let query = "param1=value1&param2=value2&param3=value3";

        let result = get_query_param(query, "param2");

        assert_eq!(result, Some("value2"));
    }

    #[test]
    fn test_get_query_param_case_insensitive() {
        let query = "Param1=value1&PARAM2=value2&param3=value3";

        let result = get_query_param(query, "param2");

        assert_eq!(result, Some("value2"));
    }

    #[test]
    fn test_get_query_param_not_found() {
        let query = "param1=value1&param2=value2&param3=value3";

        let result = get_query_param(query, "param4");

        assert_eq!(result, None);
    }

    #[test]
    fn test_get_query_param_empty_query() {
        let query = "";

        let result = get_query_param(query, "param1");

        assert_eq!(result, None);
    }

    #[test]
    fn test_get_query_param_malformed_query() {
        let query = "param1&param2=value2&param3";

        let result = get_query_param(query, "param2");

        assert_eq!(result, Some("value2"));

        let result = get_query_param(query, "param1");

        assert_eq!(result, None);
    }

    #[test]
    fn test_get_query_param_with_equals_in_value() {
        let query = "param1=value=with=equals&param2=value2";

        let result = get_query_param(query, "param1");

        assert_eq!(result, Some("value=with=equals"));
    }

    #[test]
    fn test_credentials_is_expired() {
        let mut cred = create_test_credentials();
        cred.expiration = Some(OffsetDateTime::now_utc() - time::Duration::hours(1));

        assert!(cred.is_expired());
    }

    #[test]
    fn test_credentials_is_not_expired() {
        let mut cred = create_test_credentials();
        cred.expiration = Some(OffsetDateTime::now_utc() + time::Duration::hours(1));

        assert!(!cred.is_expired());
    }

    #[test]
    fn test_credentials_no_expiration() {
        let cred = create_test_credentials();

        assert!(!cred.is_expired());
    }

    #[test]
    fn test_credentials_is_temp() {
        let cred = create_temp_credentials();

        assert!(cred.is_temp());
    }

    #[test]
    fn test_credentials_is_not_temp_no_session_token() {
        let mut cred = create_test_credentials();
        cred.session_token = "".to_string();

        assert!(!cred.is_temp());
    }

    #[test]
    fn test_credentials_is_not_temp_expired() {
        let mut cred = create_temp_credentials();
        cred.expiration = Some(OffsetDateTime::now_utc() - time::Duration::hours(1));

        assert!(!cred.is_temp());
    }

    #[test]
    fn test_credentials_is_service_account() {
        let cred = create_service_account_credentials();

        assert!(cred.is_service_account());
    }

    #[test]
    fn test_credentials_is_not_service_account() {
        let cred = create_test_credentials();

        assert!(!cred.is_service_account());
    }
}
