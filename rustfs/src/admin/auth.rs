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

use crate::auth::get_condition_values;
use http::HeaderMap;
use rustfs_credentials::Credentials;
use rustfs_iam::store::object::ObjectStore;
use rustfs_iam::sys::IamSys;
use rustfs_policy::policy::Args;
use rustfs_policy::policy::action::Action;
use s3s::S3Result;
use s3s::s3_error;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone)]
struct AuthContext<'a> {
    headers: &'a HeaderMap,
    cred: &'a Credentials,
    is_owner: bool,
    deny_only: bool,
    remote_addr: Option<std::net::SocketAddr>,
}

pub async fn validate_admin_request(
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
    remote_addr: Option<std::net::SocketAddr>,
) -> S3Result<()> {
    let Ok(iam_store) = rustfs_iam::get() else {
        return Err(s3_error!(InternalError, "iam not init"));
    };
    let ctx = AuthContext {
        headers,
        cred,
        is_owner,
        deny_only,
        remote_addr,
    };

    for action in &actions {
        if check_admin_request_auth(iam_store.clone(), &ctx, *action, "", "")
            .await
            .is_ok()
        {
            return Ok(());
        }
    }
    Err(s3_error!(AccessDenied, "Access Denied"))
}

async fn check_admin_request_auth(
    iam_store: Arc<IamSys<ObjectStore>>,
    ctx: &AuthContext<'_>,
    action: Action,
    bucket: &str,
    object: &str,
) -> S3Result<()> {
    let conditions = get_condition_values(ctx.headers, ctx.cred, None, None, ctx.remote_addr);

    if !iam_store
        .is_allowed(&Args {
            account: &ctx.cred.access_key,
            groups: &ctx.cred.groups,
            action,
            conditions: &conditions,
            is_owner: ctx.is_owner,
            claims: ctx.cred.claims.as_ref().unwrap_or(&HashMap::new()),
            deny_only: ctx.deny_only,
            bucket,
            object,
        })
        .await
    {
        return Err(s3_error!(AccessDenied, "Access Denied"));
    }

    Ok(())
}

pub async fn validate_admin_request_with_bucket(
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
    remote_addr: Option<std::net::SocketAddr>,
    bucket: &str,
) -> S3Result<()> {
    let Ok(iam_store) = rustfs_iam::get() else {
        return Err(s3_error!(InternalError, "iam not init"));
    };
    let ctx = AuthContext {
        headers,
        cred,
        is_owner,
        deny_only,
        remote_addr,
    };

    for action in &actions {
        if check_admin_request_auth(iam_store.clone(), &ctx, *action, bucket, "")
            .await
            .is_ok()
        {
            return Ok(());
        }
    }
    Err(s3_error!(AccessDenied, "Access Denied"))
}
