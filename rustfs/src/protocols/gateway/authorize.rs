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

use super::action::S3Action;
use super::adapter::is_operation_supported;
use crate::protocols::session::context::SessionContext;
use rustfs_credentials;
use rustfs_iam::get;
use rustfs_policy::policy::Args;
use std::collections::HashMap;
use tracing::{debug, error};

/// Check if a principal is allowed to perform an S3 action
pub async fn is_authorized(session_context: &SessionContext, action: &S3Action, bucket: &str, object: Option<&str>) -> bool {
    let iam_sys = match get() {
        Ok(sys) => sys,
        Err(e) => {
            error!("IAM system unavailable: {}", e);
            return false;
        }
    };

    // Create policy arguments
    let mut claims = HashMap::new();
    claims.insert(
        "principal".to_string(),
        serde_json::Value::String(session_context.principal.access_key().to_string()),
    );

    let policy_action: rustfs_policy::policy::action::Action = action.clone().into();

    // Check if user is the owner (admin)
    let is_owner = if let Some(global_cred) = rustfs_credentials::get_global_action_cred() {
        session_context.principal.access_key() == global_cred.access_key
    } else {
        false
    };

    let args = Args {
        account: session_context.principal.access_key(),
        groups: &session_context.principal.user_identity.credentials.groups,
        action: policy_action,
        bucket,
        conditions: &HashMap::new(),
        is_owner,
        object: object.unwrap_or(""),
        claims: &claims,
        deny_only: false,
    };

    debug!(
        "FTPS AUTH - Checking authorization: account={}, action={:?}, bucket='{}', object={:?}",
        args.account, args.action, args.bucket, args.object
    );

    let allowed = iam_sys.is_allowed(&args).await;
    debug!("FTPS AUTH - Authorization result: {}", allowed);
    allowed
}

/// Unified authorization entry point for all protocols
pub async fn authorize_operation(
    session_context: &SessionContext,
    action: &S3Action,
    bucket: &str,
    object: Option<&str>,
) -> Result<(), AuthorizationError> {
    // First check if the operation is supported
    if !is_operation_supported(session_context.protocol.clone(), action) {
        return Err(AuthorizationError::AccessDenied);
    }

    // Then check IAM authorization
    if is_authorized(session_context, action, bucket, object).await {
        Ok(())
    } else {
        Err(AuthorizationError::AccessDenied)
    }
}

/// Authorization errors
#[derive(Debug, thiserror::Error)]
pub enum AuthorizationError {
    #[error("Access denied")]
    AccessDenied,
}
