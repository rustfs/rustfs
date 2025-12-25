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

//! Authorization for protocol gateway
//!
//! This module provides unified authorization for all protocol implementations.
//! It ensures that all protocols use the same IAM system and policy enforcement
//! as the S3 API.
//!
//! MINIO CONSTRAINT: All protocol access control MUST use the same authentication path
//! as external clients and MUST NOT bypass S3 policy enforcement.

use super::action::S3Action;
use super::adapter::is_operation_supported;
use crate::protocols::session::context::SessionContext;
use rustfs_iam::get;
use rustfs_policy::policy::Args;
use std::collections::HashMap;

/// Check if a principal is allowed to perform an S3 action
///
/// MINIO CONSTRAINT: Must use the same policy enforcement as external clients
pub async fn is_authorized(session_context: &SessionContext, action: &S3Action, bucket: &str, object: Option<&str>) -> bool {
    let iam_sys = match get() {
        Ok(sys) => sys,
        Err(_) => return false,
    };

    // Create policy arguments
    let mut claims = HashMap::new();
    claims.insert("principal".to_string(), serde_json::Value::String(session_context.principal.access_key().to_string()));

    let policy_action: rustfs_policy::policy::action::Action = action.clone().into();
    let args = Args {
        account: session_context.principal.access_key(),
        groups: &session_context.principal.user_identity.credentials.groups,
        action: policy_action,
        bucket,
        conditions: &HashMap::new(),
        is_owner: false,
        object: object.unwrap_or(""),
        claims: &claims,
        deny_only: false,
    };

    iam_sys.is_allowed(&args).await
}

/// Unified authorization entry point for all protocols
///
/// MINIO CONSTRAINT: This is the single authorization entry point for all protocols
pub async fn authorize_operation(
    session_context: &SessionContext,
    action: &S3Action,
    bucket: &str,
    object: Option<&str>,
) -> Result<(), AuthorizationError> {
    // First check if the operation is supported
    // This is where protocol-specific limitations would be enforced
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
///
/// MINIO CONSTRAINT: Must use S3-compatible error codes
#[derive(Debug, thiserror::Error)]
pub enum AuthorizationError {
    #[error("Access denied")]
    AccessDenied,
    #[error("Invalid principal")]
    InvalidPrincipal,
}