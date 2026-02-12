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

/// Permission mode when multiple actions are given.
#[derive(Clone, Copy, Default)]
pub enum AdminPermissionMode {
    /// Any of the listed actions is sufficient to allow the request.
    #[default]
    Any,
    /// All of the listed actions must be allowed.
    All,
}

pub async fn validate_admin_request(
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
    mode: AdminPermissionMode,
    remote_addr: Option<std::net::SocketAddr>,
) -> S3Result<()> {
    let Ok(iam_store) = rustfs_iam::get() else {
        return Err(s3_error!(InternalError, "iam not init"));
    };
    match mode {
        AdminPermissionMode::Any => {
            for action in &actions {
                if check_admin_request_auth(iam_store.clone(), headers, cred, is_owner, deny_only, *action, remote_addr)
                    .await
                    .is_ok()
                {
                    return Ok(());
                }
            }
            Err(s3_error!(AccessDenied, "Access Denied"))
        }
        AdminPermissionMode::All => {
            for action in &actions {
                check_admin_request_auth(iam_store.clone(), headers, cred, is_owner, deny_only, *action, remote_addr).await?;
            }
            Ok(())
        }
    }
}

async fn check_admin_request_auth(
    iam_store: Arc<IamSys<ObjectStore>>,
    headers: &HeaderMap,
    cred: &Credentials,
    is_owner: bool,
    deny_only: bool,
    action: Action,
    remote_addr: Option<std::net::SocketAddr>,
) -> S3Result<()> {
    let conditions = get_condition_values(headers, cred, None, None, remote_addr);

    if !iam_store
        .is_allowed(&Args {
            account: &cred.access_key,
            groups: &cred.groups,
            action,
            conditions: &conditions,
            is_owner,
            claims: cred.claims.as_ref().unwrap_or(&HashMap::new()),
            deny_only,
            bucket: "",
            object: "",
        })
        .await
    {
        return Err(s3_error!(AccessDenied, "Access Denied"));
    }

    Ok(())
}
