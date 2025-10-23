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

use crate::{
    admin::router::Operation,
    auth::{check_key_valid, get_session_token},
};
use http::StatusCode;
use matchit::Params;
use rustfs_ecstore::bucket::utils::serialize;
use rustfs_iam::{manager::get_token_signing_key, sys::SESSION_POLICY_NAME};
use rustfs_policy::{auth::get_new_credentials_with_metadata, policy::Policy};
use s3s::{
    Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result,
    dto::{AssumeRoleOutput, Credentials, Timestamp},
    s3_error,
};
use serde::Deserialize;
use serde_json::Value;
use serde_urlencoded::from_bytes;
use std::collections::HashMap;
use time::{Duration, OffsetDateTime};
use tracing::{info, warn};

const ASSUME_ROLE_ACTION: &str = "AssumeRole";
const ASSUME_ROLE_VERSION: &str = "2011-06-15";

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "PascalCase", default)]
pub struct AssumeRoleRequest {
    pub action: String,
    pub duration_seconds: usize,
    pub version: String,
    pub role_arn: String,
    pub role_session_name: String,
    pub policy: String,
    pub external_id: String,
}

pub struct AssumeRoleHandle {}
#[async_trait::async_trait]
impl Operation for AssumeRoleHandle {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle AssumeRoleHandle");

        let Some(user) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };

        let session_token = get_session_token(&req.uri, &req.headers);
        if session_token.is_some() {
            return Err(s3_error!(InvalidRequest, "AccessDenied1"));
        }

        let (cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &user.access_key).await?;

        // TODO: Check permissions, do not allow STS access
        if cred.is_temp() || cred.is_service_account() {
            return Err(s3_error!(InvalidRequest, "AccessDenied"));
        }

        let mut input = req.input;

        let bytes = match input.store_all_unlimited().await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "get body failed"));
            }
        };

        let body: AssumeRoleRequest = from_bytes(&bytes).map_err(|_e| s3_error!(InvalidRequest, "get body failed"))?;

        if body.action.as_str() != ASSUME_ROLE_ACTION {
            return Err(s3_error!(InvalidArgument, "not support action"));
        }

        if body.version.as_str() != ASSUME_ROLE_VERSION {
            return Err(s3_error!(InvalidArgument, "not support version"));
        }

        let mut claims = cred.claims.unwrap_or_default();

        populate_session_policy(&mut claims, &body.policy)?;

        let exp = {
            if body.duration_seconds > 0 {
                body.duration_seconds
            } else {
                3600
            }
        };

        claims.insert(
            "exp".to_string(),
            Value::Number(serde_json::Number::from(OffsetDateTime::now_utc().unix_timestamp() + exp as i64)),
        );

        claims.insert("parent".to_string(), Value::String(cred.access_key.clone()));

        // warn!("AssumeRole get cred {:?}", &user);
        // warn!("AssumeRole get body {:?}", &body);

        let Ok(iam_store) = rustfs_iam::get() else {
            return Err(s3_error!(InvalidRequest, "iam not init"));
        };

        if let Err(_err) = iam_store.policy_db_get(&cred.access_key, &cred.groups).await {
            return Err(s3_error!(InvalidArgument, "invalid policy arg"));
        }

        let Some(secret) = get_token_signing_key() else {
            return Err(s3_error!(InvalidArgument, "global active sk not init"));
        };

        info!("AssumeRole get claims {:?}", &claims);

        let mut new_cred = get_new_credentials_with_metadata(&claims, &secret)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("get new cred failed {e}")))?;

        new_cred.parent_user = cred.access_key.clone();

        info!("AssumeRole get new_cred {:?}", &new_cred);

        if let Err(_err) = iam_store.set_temp_user(&new_cred.access_key, &new_cred, None).await {
            return Err(s3_error!(InternalError, "set_temp_user failed"));
        }

        // TODO: globalSiteReplicationSys

        let resp = AssumeRoleOutput {
            credentials: Some(Credentials {
                access_key_id: new_cred.access_key,
                expiration: Timestamp::from(
                    new_cred
                        .expiration
                        .unwrap_or(OffsetDateTime::now_utc().saturating_add(Duration::seconds(3600))),
                ),
                secret_access_key: new_cred.secret_key,
                session_token: new_cred.session_token,
            }),
            ..Default::default()
        };

        // getAssumeRoleCredentials
        let output = serialize::<AssumeRoleOutput>(&resp).unwrap();

        Ok(S3Response::new((StatusCode::OK, Body::from(output))))
    }
}

pub fn populate_session_policy(claims: &mut HashMap<String, Value>, policy: &str) -> S3Result<()> {
    if !policy.is_empty() {
        let session_policy = Policy::parse_config(policy.as_bytes())
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("parse policy err {e}")))?;
        if session_policy.version.is_empty() {
            return Err(s3_error!(InvalidRequest, "invalid policy"));
        }

        let policy_buf = serde_json::to_vec(&session_policy)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal policy err {e}")))?;

        if policy_buf.len() > 2048 {
            return Err(s3_error!(InvalidRequest, "policy too large"));
        }

        claims.insert(
            SESSION_POLICY_NAME.to_string(),
            Value::String(base64_simd::URL_SAFE_NO_PAD.encode_to_string(&policy_buf)),
        );
    }

    Ok(())
}
