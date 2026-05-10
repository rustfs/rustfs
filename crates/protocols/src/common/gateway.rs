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

use rustfs_credentials;
use rustfs_policy::policy::action::S3Action as PolicyS3Action;
use serde_json;
use std::collections::HashMap;
use thiserror::Error;
use tracing::error;

use super::session::SessionContext;

/// Authorization errors
#[derive(Debug, Error)]
pub enum AuthorizationError {
    /// Policy denied the principal the requested action. Distinct
    /// from IamUnavailable so protocol drivers can map a deny to
    /// PermissionDenied while mapping a transient IAM outage to
    /// the spec-equivalent Failure (no SFTPv3 service-unavailable
    /// status exists).
    #[error("Access denied")]
    AccessDenied,

    /// The IAM layer was unreachable or returned an error other
    /// than the expected Allow/Deny verdict. Indistinguishable
    /// from AccessDenied at the wire boundary in earlier
    /// implementations; protocol drivers now branch on this
    /// variant to surface a warn log naming the failing
    /// operation so operators can correlate session errors with
    /// IAM degradation.
    #[error("IAM system unavailable")]
    IamUnavailable,
}

/// S3 actions that can be performed through the gateway
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum S3Action {
    // Bucket operations
    CreateBucket,
    DeleteBucket,
    ListBucket,
    ListBuckets,
    HeadBucket,

    // Object operations
    GetObject,
    PutObject,
    DeleteObject,
    HeadObject,
    CopyObject,

    // Multipart operations
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
    ListMultipartUploads,
    ListParts,

    // ACL operations
    GetBucketAcl,
    PutBucketAcl,
    GetObjectAcl,
    PutObjectAcl,
}

impl From<S3Action> for PolicyS3Action {
    fn from(action: S3Action) -> Self {
        match action {
            S3Action::CreateBucket => PolicyS3Action::CreateBucketAction,
            S3Action::DeleteBucket => PolicyS3Action::DeleteBucketAction,
            S3Action::ListBucket => PolicyS3Action::ListBucketAction,
            S3Action::ListBuckets => PolicyS3Action::ListAllMyBucketsAction,
            S3Action::HeadBucket => PolicyS3Action::HeadBucketAction,
            S3Action::GetObject => PolicyS3Action::GetObjectAction,
            S3Action::PutObject => PolicyS3Action::PutObjectAction,
            S3Action::DeleteObject => PolicyS3Action::DeleteObjectAction,
            S3Action::HeadObject => PolicyS3Action::GetObjectAction,
            S3Action::CreateMultipartUpload => PolicyS3Action::PutObjectAction,
            S3Action::UploadPart => PolicyS3Action::PutObjectAction,
            S3Action::CompleteMultipartUpload => PolicyS3Action::PutObjectAction,
            S3Action::AbortMultipartUpload => PolicyS3Action::AbortMultipartUploadAction,
            S3Action::ListMultipartUploads => PolicyS3Action::ListBucketMultipartUploadsAction,
            S3Action::ListParts => PolicyS3Action::ListMultipartUploadPartsAction,
            S3Action::GetBucketAcl => PolicyS3Action::GetBucketAclAction,
            S3Action::PutBucketAcl => PolicyS3Action::PutBucketAclAction,
            S3Action::GetObjectAcl => PolicyS3Action::GetObjectAclAction,
            S3Action::PutObjectAcl => PolicyS3Action::PutObjectAclAction,
            S3Action::CopyObject => PolicyS3Action::PutObjectAction,
        }
    }
}

impl From<S3Action> for rustfs_policy::policy::action::Action {
    fn from(action: S3Action) -> Self {
        rustfs_policy::policy::action::Action::S3Action(action.into())
    }
}

impl S3Action {
    /// Get the string representation of the action
    pub fn as_str(&self) -> &'static str {
        match self {
            S3Action::CreateBucket => "s3:CreateBucket",
            S3Action::DeleteBucket => "s3:DeleteBucket",
            S3Action::ListBucket => "s3:ListBucket",
            S3Action::ListBuckets => "s3:ListAllMyBuckets",
            S3Action::HeadBucket => "s3:ListBucket",
            S3Action::GetObject => "s3:GetObject",
            S3Action::PutObject => "s3:PutObject",
            S3Action::DeleteObject => "s3:DeleteObject",
            S3Action::HeadObject => "s3:GetObject",
            S3Action::CreateMultipartUpload => "s3:PutObject",
            S3Action::UploadPart => "s3:PutObject",
            S3Action::CompleteMultipartUpload => "s3:PutObject",
            S3Action::AbortMultipartUpload => "s3:AbortMultipartUpload",
            S3Action::ListMultipartUploads => "s3:ListBucketMultipartUploads",
            S3Action::ListParts => "s3:ListMultipartUploadParts",
            S3Action::GetBucketAcl => "s3:GetBucketAcl",
            S3Action::PutBucketAcl => "s3:PutBucketAcl",
            S3Action::GetObjectAcl => "s3:GetObjectAcl",
            S3Action::PutObjectAcl => "s3:PutObjectAcl",
            S3Action::CopyObject => "s3:PutObject",
        }
    }
}

/// Check if operation is supported for the given protocol
pub fn is_operation_supported(protocol: super::session::Protocol, action: &S3Action) -> bool {
    match protocol {
        super::session::Protocol::Ftps => match action {
            // Bucket operations
            S3Action::CreateBucket => true,
            S3Action::DeleteBucket => true,

            // Object operations
            S3Action::GetObject => true,    // RETR command
            S3Action::PutObject => true,    // STOR and APPE commands both map to PutObject
            S3Action::DeleteObject => true, // DELE command
            S3Action::HeadObject => true,   // SIZE command

            // Multipart operations
            S3Action::CreateMultipartUpload => false,
            S3Action::UploadPart => false,
            S3Action::CompleteMultipartUpload => false,
            S3Action::AbortMultipartUpload => false,
            S3Action::ListMultipartUploads => false,
            S3Action::ListParts => false,

            // ACL operations
            S3Action::GetBucketAcl => false,
            S3Action::PutBucketAcl => false,
            S3Action::GetObjectAcl => false,
            S3Action::PutObjectAcl => false,

            // Other operations
            S3Action::CopyObject => false, // No native copy support in FTPS
            S3Action::ListBucket => true,  // LIST command
            S3Action::ListBuckets => true, // LIST at root level
            S3Action::HeadBucket => true,  // Can check if directory exists
        },
        super::session::Protocol::Swift => match action {
            // Swift supports most S3 operations via translation
            S3Action::CreateBucket => true, // PUT container
            S3Action::DeleteBucket => true, // DELETE container
            S3Action::GetObject => true,    // GET object
            S3Action::PutObject => true,    // PUT object
            S3Action::DeleteObject => true, // DELETE object
            S3Action::HeadObject => true,   // HEAD object
            S3Action::CopyObject => true,   // COPY method
            S3Action::ListBucket => true,   // GET container
            S3Action::ListBuckets => true,  // GET account
            S3Action::HeadBucket => true,   // HEAD container

            // Multipart not directly supported by Swift API (uses different approach)
            S3Action::CreateMultipartUpload => false,
            S3Action::UploadPart => false,
            S3Action::CompleteMultipartUpload => false,
            S3Action::AbortMultipartUpload => false,
            S3Action::ListMultipartUploads => false,
            S3Action::ListParts => false,

            // ACL operations not supported by Swift API (uses different model)
            S3Action::GetBucketAcl => false,
            S3Action::PutBucketAcl => false,
            S3Action::GetObjectAcl => false,
            S3Action::PutObjectAcl => false,
        },
        super::session::Protocol::WebDav => match action {
            // Bucket operations
            S3Action::CreateBucket => true, // MKCOL at root level
            S3Action::DeleteBucket => true, // DELETE at root level
            S3Action::ListBucket => true,   // PROPFIND
            S3Action::ListBuckets => true,  // PROPFIND at root
            S3Action::HeadBucket => true,   // PROPFIND/HEAD

            // Object operations
            S3Action::GetObject => true,    // GET
            S3Action::PutObject => true,    // PUT
            S3Action::DeleteObject => true, // DELETE
            S3Action::HeadObject => true,   // HEAD/PROPFIND
            S3Action::CopyObject => false,  // COPY (not implemented yet)

            // Multipart operations (not supported in WebDAV)
            S3Action::CreateMultipartUpload => false,
            S3Action::UploadPart => false,
            S3Action::CompleteMultipartUpload => false,
            S3Action::AbortMultipartUpload => false,
            S3Action::ListMultipartUploads => false,
            S3Action::ListParts => false,

            // ACL operations (not supported in WebDAV)
            S3Action::GetBucketAcl => false,
            S3Action::PutBucketAcl => false,
            S3Action::GetObjectAcl => false,
            S3Action::PutObjectAcl => false,
        },
        super::session::Protocol::Sftp => match action {
            // Bucket operations: SFTP exposes top-level buckets as directories.
            S3Action::CreateBucket => true, // MKDIR at the root
            S3Action::DeleteBucket => true, // RMDIR at the root
            S3Action::ListBucket => true,   // OPENDIR/READDIR within a bucket
            S3Action::ListBuckets => true,  // OPENDIR/READDIR at the root
            S3Action::HeadBucket => true,   // STAT/LSTAT of a bucket entry

            // Object operations
            S3Action::GetObject => true,    // OPEN/READ
            S3Action::PutObject => true,    // OPEN(WRITE)/WRITE/CLOSE
            S3Action::DeleteObject => true, // REMOVE
            S3Action::HeadObject => true,   // STAT/LSTAT/FSTAT
            S3Action::CopyObject => true,   // RENAME maps to copy + delete

            // Multipart operations: streamed PUT path used by the write driver.
            S3Action::CreateMultipartUpload => true,
            S3Action::UploadPart => true,
            S3Action::CompleteMultipartUpload => true,
            S3Action::AbortMultipartUpload => true,
            S3Action::ListMultipartUploads => false,
            S3Action::ListParts => false,

            // ACL operations: SFTP has no equivalent surface.
            S3Action::GetBucketAcl => false,
            S3Action::PutBucketAcl => false,
            S3Action::GetObjectAcl => false,
            S3Action::PutObjectAcl => false,
        },
    }
}

/// Check if a principal is allowed to perform an S3 action.
/// Returns Ok(true) when the policy allows the action, Ok(false) when
/// the policy denies it, and Err(AuthorizationError::IamUnavailable)
/// when the IAM layer is unreachable (rustfs_iam::get fails). The
/// IamUnavailable case is distinct from a Deny so protocol drivers
/// can return a transient-failure status with a warn log instead of
/// the permanent permission-denied status that a Deny produces.
pub async fn is_authorized(
    session_context: &SessionContext,
    action: &S3Action,
    bucket: &str,
    object: Option<&str>,
) -> Result<bool, AuthorizationError> {
    let iam_sys = match rustfs_iam::get() {
        Ok(sys) => sys,
        Err(e) => {
            error!("IAM system unavailable: {}", e);
            return Err(AuthorizationError::IamUnavailable);
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

    let args = rustfs_policy::policy::Args {
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

    Ok(iam_sys.is_allowed(&args).await)
}

/// Authorize an operation and return an error if not authorized.
/// AccessDenied covers both the protocol-not-supported case and the
/// policy-denies case. IamUnavailable propagates from is_authorized
/// when the IAM layer is unreachable; protocol drivers map it to a
/// transient-failure status with a warn log rather than the
/// permanent permission-denied status that AccessDenied produces.
pub async fn authorize_operation(
    session_context: &SessionContext,
    action: &S3Action,
    bucket: &str,
    object: Option<&str>,
) -> Result<(), AuthorizationError> {
    // SECURITY: the next two lines are cfg(test)-gated. Release builds strip
    // them and run only the IAM path below. Implementation and verification
    // recipe are in the test_auth_override submodule at the bottom of this file.
    #[cfg(test)]
    if let Some(decision) = test_auth_override::consult(action, bucket, object) {
        return decision;
    }

    // check if the operation is supported
    if !is_operation_supported(session_context.protocol, action) {
        return Err(AuthorizationError::AccessDenied);
    }

    // check IAM authorization
    match is_authorized(session_context, action, bucket, object).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(AuthorizationError::AccessDenied),
        Err(e) => Err(e),
    }
}

/// Test-only authorisation override for driver-level unit tests.
///
/// Every item in this module is gated on #[cfg(test)], and the single
/// call site in authorize_operation is also #[cfg(test)]-gated, so
/// release builds contain none of this code and run only the IAM path.
///
/// A unit test installs a decide closure via with_test_auth_override,
/// runs an async body that calls authorize_operation, and the override
/// is cleared on scope exit by a Drop guard so a panic inside the body
/// cannot leak the decision into later tests on the same thread.
#[cfg(test)]
pub mod test_auth_override {
    use super::{AuthorizationError, S3Action};
    use std::cell::{Cell, RefCell};

    type DecideFn = Box<dyn Fn(&S3Action, &str, Option<&str>) -> bool>;

    thread_local! {
        /// Current per-thread Allow/Deny override. None means no test
        /// has installed one and authorize_operation falls through to
        /// its IAM path.
        static OVERRIDE: RefCell<Option<DecideFn>> = const { RefCell::new(None) };

        /// Per-thread IAM-unavailable injection. When true, consult
        /// short-circuits with IamUnavailable so tests can verify the
        /// IAM-outage branch without standing up a real degraded IAM
        /// fixture. Takes precedence over the Allow/Deny OVERRIDE.
        static IAM_UNAVAILABLE: Cell<bool> = const { Cell::new(false) };
    }

    /// Consult the per-thread overrides. IamUnavailable takes
    /// precedence over the Allow/Deny override so a test combining
    /// both flags can verify that the unavailable branch fires before
    /// any policy evaluation. Returns Some(decision) when any
    /// override is active on the current thread, None otherwise.
    /// Called exclusively from authorize_operation's cfg(test)-gated
    /// fast path.
    pub(super) fn consult(action: &S3Action, bucket: &str, object: Option<&str>) -> Option<Result<(), AuthorizationError>> {
        if IAM_UNAVAILABLE.with(|c| c.get()) {
            return Some(Err(AuthorizationError::IamUnavailable));
        }
        OVERRIDE.with(|cell| {
            cell.borrow().as_ref().map(|decide| {
                if decide(action, bucket, object) {
                    Ok(())
                } else {
                    Err(AuthorizationError::AccessDenied)
                }
            })
        })
    }

    /// Install a test-only authorisation decision for the duration of the
    /// supplied async body, then clear it. A Drop guard performs the
    /// clearing so a panic inside the body does not leak the decision
    /// into later tests on the same thread.
    ///
    /// Example:
    ///     let result = with_test_auth_override(
    ///         |_action, _bucket, _object| true,
    ///         async { authorize_operation(&ctx, &action, "b", None).await },
    ///     ).await;
    pub async fn with_test_auth_override<Fut, R>(decide: impl Fn(&S3Action, &str, Option<&str>) -> bool + 'static, body: Fut) -> R
    where
        Fut: std::future::Future<Output = R>,
    {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                OVERRIDE.with(|cell| *cell.borrow_mut() = None);
            }
        }
        OVERRIDE.with(|cell| *cell.borrow_mut() = Some(Box::new(decide)));
        let _reset = Reset;
        body.await
    }

    /// Inject AuthorizationError::IamUnavailable for every
    /// authorize_operation call inside the supplied async body, then
    /// clear the flag on scope exit (Drop guard handles the panic
    /// case). Used by the IAM-outage tests that verify protocol
    /// drivers map the unreachable variant to a transient-failure
    /// status with a warn log rather than to PermissionDenied.
    pub async fn with_test_iam_unavailable<Fut, R>(body: Fut) -> R
    where
        Fut: std::future::Future<Output = R>,
    {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                IAM_UNAVAILABLE.with(|c| c.set(false));
            }
        }
        IAM_UNAVAILABLE.with(|c| c.set(true));
        let _reset = Reset;
        body.await
    }
}

/// Ergonomic re-export so tests reach the helpers via
/// common::gateway::with_test_auth_override rather than nesting
/// the submodule path.
#[cfg(test)]
pub use test_auth_override::{with_test_auth_override, with_test_iam_unavailable};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::session::{Protocol, ProtocolPrincipal, SessionContext};
    use rustfs_policy::auth::UserIdentity;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;

    fn test_session() -> SessionContext {
        let principal = ProtocolPrincipal::new(Arc::new(UserIdentity::default()));
        SessionContext::new(principal, Protocol::Sftp, IpAddr::V4(Ipv4Addr::LOCALHOST))
    }

    #[tokio::test]
    async fn with_test_auth_override_allow_returns_ok() {
        let session = test_session();
        let result = with_test_auth_override(|_action, _bucket, _object| true, async {
            authorize_operation(&session, &S3Action::GetObject, "b", None).await
        })
        .await;
        assert!(result.is_ok(), "override returning true must make authorize_operation succeed");
    }

    #[tokio::test]
    async fn with_test_auth_override_deny_returns_err() {
        let session = test_session();
        let result = with_test_auth_override(|_action, _bucket, _object| false, async {
            authorize_operation(&session, &S3Action::PutObject, "b", Some("k")).await
        })
        .await;
        assert!(matches!(result, Err(AuthorizationError::AccessDenied)));
    }

    #[tokio::test]
    async fn with_test_auth_override_clears_after_body() {
        let session = test_session();
        // Discard the body Result. The test exercises the clear-on-return
        // side-effect of with_test_auth_override, not the body's outcome.
        let _ = with_test_auth_override(|_, _, _| true, async { Result::<(), ()>::Ok(()) }).await;
        // After the helper returns, the IAM path runs. IAM is not
        // initialised in this test binary, so is_authorized returns
        // IamUnavailable. A leaked override would have produced Ok.
        let result = authorize_operation(&session, &S3Action::GetObject, "b", None).await;
        assert!(matches!(result, Err(AuthorizationError::IamUnavailable)));
    }

    #[tokio::test]
    async fn with_test_auth_override_closure_sees_action_bucket_object() {
        let session = test_session();
        let result = with_test_auth_override(
            |action, bucket, object| {
                matches!(action, S3Action::UploadPart) && bucket == "only-this-bucket" && object == Some("only-this-key")
            },
            async {
                let allowed =
                    authorize_operation(&session, &S3Action::UploadPart, "only-this-bucket", Some("only-this-key")).await;
                let denied_by_action =
                    authorize_operation(&session, &S3Action::GetObject, "only-this-bucket", Some("only-this-key")).await;
                let denied_by_bucket =
                    authorize_operation(&session, &S3Action::UploadPart, "other-bucket", Some("only-this-key")).await;
                (allowed, denied_by_action, denied_by_bucket)
            },
        )
        .await;
        assert!(result.0.is_ok());
        assert!(matches!(result.1, Err(AuthorizationError::AccessDenied)));
        assert!(matches!(result.2, Err(AuthorizationError::AccessDenied)));
    }

    /// Regression guard for the SECURITY invariant: the test override
    /// is reachable only under cfg(test). The body depends on items in
    /// the test_auth_override module, so if a future edit moves any of
    /// those items out of a cfg(test) gate the build of THIS test
    /// binary still succeeds (cfg(test) is active here) but the
    /// reviewer recipe documented in test_auth_override's module
    /// comment will start reporting matches in release expansion. Run
    /// the recipe before shipping.
    #[tokio::test]
    async fn override_roundtrip_confirms_consult_path_under_cfg_test() {
        let session = test_session();

        // Without an installed override, consult returns None and the
        // IAM path runs. IAM is not initialised in tests so the path
        // returns IamUnavailable.
        let without = authorize_operation(&session, &S3Action::GetObject, "b", None).await;
        assert!(matches!(without, Err(AuthorizationError::IamUnavailable)));

        // With an installed override, consult returns Some and
        // authorize_operation returns immediately with the override's
        // decision, bypassing the IAM path.
        let with = with_test_auth_override(|_, _, _| true, async {
            authorize_operation(&session, &S3Action::GetObject, "b", None).await
        })
        .await;
        assert!(with.is_ok());

        // After the scope, consult returns None again and the IAM path
        // reclaims the authorization decision.
        let after = authorize_operation(&session, &S3Action::GetObject, "b", None).await;
        assert!(matches!(after, Err(AuthorizationError::IamUnavailable)));
    }

    /// IamUnavailable is distinct from AccessDenied at the gateway
    /// boundary, so protocol drivers can branch on it. with_test_iam_unavailable
    /// short-circuits authorize_operation with the IamUnavailable
    /// variant regardless of any installed Allow/Deny override, and
    /// the precedence is documented in test_auth_override::consult.
    #[tokio::test]
    async fn with_test_iam_unavailable_returns_iam_unavailable_variant() {
        let session = test_session();
        let result = with_test_iam_unavailable(authorize_operation(&session, &S3Action::GetObject, "b", Some("k"))).await;
        assert!(matches!(result, Err(AuthorizationError::IamUnavailable)));
    }

    /// IamUnavailable beats an installed Allow override, so a test
    /// combining both flags exercises the documented precedence rule
    /// in test_auth_override::consult: a degraded IAM is observed
    /// before any policy evaluation.
    #[tokio::test]
    async fn with_test_iam_unavailable_takes_precedence_over_allow_override() {
        let session = test_session();
        let result = with_test_auth_override(
            |_, _, _| true,
            with_test_iam_unavailable(authorize_operation(&session, &S3Action::GetObject, "b", Some("k"))),
        )
        .await;
        assert!(matches!(result, Err(AuthorizationError::IamUnavailable)));
    }
}
