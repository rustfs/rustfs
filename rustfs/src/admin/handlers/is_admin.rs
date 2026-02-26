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

use crate::admin::router::Operation;
use crate::auth::{check_key_valid, constant_time_eq, get_condition_values, get_session_token};
use http::{HeaderMap, HeaderValue};
use hyper::StatusCode;
use matchit::Params;
use rustfs_credentials::get_global_action_cred;
use rustfs_policy::policy::Args;
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Serialize)]
pub struct IsAdminResponse {
    pub is_admin: bool,
    pub access_key: String,
    pub message: String,
}

pub struct IsAdminHandler {}

#[async_trait::async_trait]
impl Operation for IsAdminHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, _owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let access_key_to_check = input_cred.access_key.clone();

        // Check if the user is admin: root user check, then evaluate through the policy engine
        let is_admin = if let Some(sys_cred) = get_global_action_cred() {
            constant_time_eq(&access_key_to_check, &sys_cred.access_key)
                || constant_time_eq(&cred.parent_user, &sys_cred.access_key)
        } else {
            false
        };

        let is_admin = if is_admin {
            true
        } else {
            let empty_claims = HashMap::new();
            let iam_store = rustfs_iam::get().map_err(|_| s3_error!(InternalError, "iam not init"))?;
            let conditions = get_condition_values(&req.headers, &cred, None, None, None);
            iam_store
                .is_allowed(&Args {
                    account: &cred.access_key,
                    groups: &cred.groups,
                    action: Action::AdminAction(AdminAction::AllAdminActions),
                    conditions: &conditions,
                    is_owner: false,
                    claims: cred.claims.as_ref().unwrap_or(&empty_claims),
                    deny_only: false,
                    bucket: "",
                    object: "",
                })
                .await
        };

        let response = IsAdminResponse {
            is_admin,
            access_key: access_key_to_check,
            message: format!("User is {}an administrator", if is_admin { "" } else { "not " }),
        };

        let data = serde_json::to_vec(&response)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse IsAdminResponse failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}
