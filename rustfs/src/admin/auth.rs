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
use http::Uri;
use rustfs_credentials::Credentials;
use rustfs_iam::store::object::ObjectStore;
use rustfs_iam::sys::IamSys;
use rustfs_policy::policy::{Args, action::Action};
use s3s::{S3Result, s3_error};
use std::sync::Arc;
use tracing::debug;

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

    let allowed = iam_store
        .is_allowed(&Args {
            account: &ctx.cred.access_key,
            groups: &ctx.cred.groups,
            action,
            conditions: &conditions,
            is_owner: ctx.is_owner,
            claims: ctx.cred.claims_or_empty(),
            deny_only: ctx.deny_only,
            bucket,
            object,
        })
        .await;

    if !allowed {
        debug!(
            target = "rustfs::admin::auth",
            ?action,
            deny_only = ctx.deny_only,
            is_owner = ctx.is_owner,
            signer_access_key = %ctx.cred.access_key,
            "IAM is_allowed returned false for admin request (enable DEBUG for rustfs::admin::auth)"
        );
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

/// Unified authentication request handler for both UI and CLI
///
/// This function provides a single entry point for authentication,
/// ensuring consistent behavior between UI and CLI authentication flows.
///
/// # Arguments
/// * `headers` - HTTP request headers
/// * `uri` - Request URI
/// * `credentials` - User credentials from request (Credentials)
///
/// # Returns
/// * `Ok((Credentials, bool))` - Authentication successful, returns user credentials and is_owner flag
/// * `Err(S3Error)` - Authentication failed with error details
///
/// # Example
/// ```ignore
/// let (cred, is_owner) = authenticate_request(&req.headers, &req.uri, &input_cred).await?;
/// ```
pub async fn authenticate_request(
    headers: &HeaderMap,
    uri: &Uri,
    credentials: &s3s::auth::Credentials,
) -> S3Result<(Credentials, bool)> {
    use crate::auth::{check_key_valid, get_session_token};

    // Extract session token from request
    let session_token = get_session_token(uri, headers).unwrap_or_default();

    // Log authentication attempt for debugging
    debug!(
        "authenticate_request: processing authentication - access_key={}, has_session_token={}",
        credentials.access_key,
        !session_token.is_empty()
    );

    // Validate credentials using the core authentication function
    let result = check_key_valid(session_token, &credentials.access_key).await;

    match &result {
        Ok((cred, is_owner)) => {
            debug!(
                "authenticate_request: authentication successful - access_key={}, is_owner={}",
                cred.access_key, is_owner
            );
        }
        Err(e) => {
            tracing::warn!(
                "authenticate_request: authentication failed - access_key={}, error={}",
                credentials.access_key,
                e
            );
        }
    }

    result
}
