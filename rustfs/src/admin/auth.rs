use std::collections::HashMap;
use std::sync::Arc;

use http::HeaderMap;
use rustfs_iam::store::object::ObjectStore;
use rustfs_iam::sys::IamSys;
use rustfs_policy::auth;
use rustfs_policy::policy::Args;
use rustfs_policy::policy::action::Action;
use s3s::S3Result;
use s3s::s3_error;

use crate::auth::get_condition_values;

pub async fn validate_admin_request(
    headers: &HeaderMap,
    cred: &auth::Credentials,
    is_owner: bool,
    deny_only: bool,
    actions: Vec<Action>,
) -> S3Result<()> {
    let Ok(iam_store) = rustfs_iam::get() else {
        return Err(s3_error!(InternalError, "iam not init"));
    };
    for action in actions {
        match check_admin_request_auth(iam_store.clone(), headers, cred, is_owner, deny_only, action).await {
            Ok(_) => return Ok(()),
            Err(_) => {
                continue;
            }
        }
    }

    Err(s3_error!(AccessDenied, "Access Denied"))
}

async fn check_admin_request_auth(
    iam_store: Arc<IamSys<ObjectStore>>,
    headers: &HeaderMap,
    cred: &auth::Credentials,
    is_owner: bool,
    deny_only: bool,
    action: Action,
) -> S3Result<()> {
    let conditions = get_condition_values(headers, cred, None, None);

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
