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

//! Pre-body IAM admission for the mounted non-GET native admin operations.

use super::ClaimedHead;
use crate::admin::handlers::site_replication::reject_site_replicator_on_public_admin;
use crate::admin::{authenticate_envelope_credentials, validate_admin_request, validate_admin_request_with_bucket};
use crate::app::{RequestCredentials, RequestEnvelope};
use crate::auth::get_session_token;
use crate::shared_types::RemoteAddr;
use http::{HeaderMap, HeaderValue, header};
use rustfs_gateway::{AuthSchemeRef, AuthzRequest, IdentityOrigin, RequestContext, ResourceShape, Subject};
use rustfs_gateway_dialect_rustfs_admin::native::ops;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_s3_types::{S3Result, s3_error};
use rustfs_trusted_proxies::ClientInfo;

/// Called after the host route admission and path preconditions, before the codec receives the body.
pub(super) async fn authorize(context: &RequestContext<'_>, request: &AuthzRequest<'_>, head: &ClaimedHead) -> S3Result<()> {
    let identity = request
        .identity
        .filter(|identity| context.auth_scheme() == AuthSchemeRef::SigV4Header && identity.origin() == IdentityOrigin::AccessKey)
        .ok_or_else(|| s3_error!(AccessDenied, "header-signed admin identity required"))?;
    let accepted = context
        .headers()
        .ok_or_else(|| s3_error!(InternalError, "accepted admin headers are unavailable"))?;
    let mut headers = HeaderMap::new();
    for (name, value) in accepted.iter_raw() {
        headers.append(name.clone(), value.clone());
    }
    if !headers.contains_key(header::HOST)
        && let Some(authority) = head.uri.authority()
    {
        headers.insert(
            header::HOST,
            HeaderValue::from_str(authority.as_str())
                .map_err(|_| s3_error!(InternalError, "invalid accepted admin authority"))?,
        );
    }
    let session = get_session_token(&head.uri, &headers).map(str::to_owned);
    let credentials = RequestCredentials::without_secret_key(identity.access_key_id().to_owned(), session);
    let (credentials, owner) = authenticate_envelope_credentials(&credentials).await?;
    let Permission::Admin { action, bucket } = permission(request)? else {
        return Ok(());
    };

    let extensions = context.server_extensions();
    let peer = extensions.get::<Option<RemoteAddr>>().copied().flatten().map(|peer| peer.0);
    let source = RequestEnvelope::policy_source_addr_from(peer, extensions.get::<ClientInfo>());
    match bucket {
        Some(bucket) => {
            validate_admin_request_with_bucket(&headers, &credentials, owner, false, vec![action], source, bucket).await?
        }
        None => validate_admin_request(&headers, &credentials, owner, false, vec![action], source).await?,
    }
    if request.operation == ops::put_v3_site_replication_remove_v2::NAME {
        reject_site_replicator_on_public_admin(&credentials)?;
    }
    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum Permission<'a> {
    Caller,
    Admin { action: Action, bucket: Option<&'a str> },
}

fn permission<'a>(request: &AuthzRequest<'a>) -> S3Result<Permission<'a>> {
    if request.operation == ops::post_v4_account_permissions::NAME {
        if request.action == ops::post_v4_account_permissions::AUTH.action
            && request.resource == ResourceShape::Service
            && matches!(request.subject, Some(Subject::Caller))
        {
            return Ok(Permission::Caller);
        }
        return Err(s3_error!(AccessDenied, "permission queries require the caller subject"));
    }
    let (action, shape) = match request.operation {
        ops::delete_v3_bucket_etag_by_bucket::NAME | ops::put_v3_bucket_etag_by_bucket::NAME => {
            (AdminAction::ConfigUpdateAdminAction, ResourceShape::Bucket)
        }
        ops::put_v3_site_replication_peer_remove_v2::NAME | ops::put_v3_site_replication_remove_v2::NAME => {
            (AdminAction::SiteReplicationRemoveAction, ResourceShape::Service)
        }
        ops::delete_v3_target_by_target_type_by_target_name::NAME => (AdminAction::SetBucketTargetAction, ResourceShape::Service),
        ops::post_v4_heal_mrf_cluster_responsibilities_actions::NAME => (AdminAction::HealAdminAction, ResourceShape::Service),
        _ => return Err(s3_error!(InternalError, "unregistered native admin operation")),
    };
    let action = Action::AdminAction(action);
    if request.action != <&str>::from(&action) || request.resource != shape || request.subject.is_some() {
        return Err(s3_error!(AccessDenied, "unexpected admin authorization requirement"));
    }
    let bucket = if shape == ResourceShape::Bucket {
        Some(
            request
                .bucket
                .ok_or_else(|| s3_error!(AccessDenied, "admin bucket is unavailable"))?
                .as_str(),
        )
    } else {
        None
    };
    Ok(Permission::Admin { action, bucket })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_gateway::{BucketName, TargetOrigin};
    use rustfs_gateway_core::op::AuthRequirement;
    use rustfs_s3_types::S3ErrorCode;

    fn route<'a>(operation: &'static str, auth: AuthRequirement, bucket: Option<&'a BucketName>) -> AuthzRequest<'a> {
        AuthzRequest {
            operation,
            action: auth.action,
            resource: auth.resource,
            bucket,
            key: None,
            copy_source_identity: None,
            version_id: None,
            route_action: auth.action,
            route_bucket: bucket,
            route_key: None,
            identity: None,
            target_origin: TargetOrigin::Path,
            subject: None,
        }
    }

    #[test]
    fn etag_write_permissions_use_the_bucket_bound_by_each_operation() {
        for (operation, auth) in [
            (ops::put_v3_bucket_etag_by_bucket::NAME, ops::put_v3_bucket_etag_by_bucket::AUTH),
            (ops::delete_v3_bucket_etag_by_bucket::NAME, ops::delete_v3_bucket_etag_by_bucket::AUTH),
        ] {
            for name in ["first-bucket", "second-bucket"] {
                let bucket = BucketName::new(name).expect("valid bucket name");
                let request = route(operation, auth, Some(&bucket));
                assert_eq!(
                    permission(&request).unwrap(),
                    Permission::Admin {
                        action: Action::AdminAction(AdminAction::ConfigUpdateAdminAction),
                        bucket: Some(name),
                    }
                );
            }
            assert_eq!(permission(&route(operation, auth, None)).unwrap_err().code(), &S3ErrorCode::AccessDenied);
        }
    }

    #[test]
    fn service_caller_and_deferred_get_permissions_stay_fail_closed() {
        for (operation, auth, expected) in [
            (
                ops::post_v4_heal_mrf_cluster_responsibilities_actions::NAME,
                ops::post_v4_heal_mrf_cluster_responsibilities_actions::AUTH,
                AdminAction::HealAdminAction,
            ),
            (
                ops::delete_v3_target_by_target_type_by_target_name::NAME,
                ops::delete_v3_target_by_target_type_by_target_name::AUTH,
                AdminAction::SetBucketTargetAction,
            ),
            (
                ops::put_v3_site_replication_peer_remove_v2::NAME,
                ops::put_v3_site_replication_peer_remove_v2::AUTH,
                AdminAction::SiteReplicationRemoveAction,
            ),
            (
                ops::put_v3_site_replication_remove_v2::NAME,
                ops::put_v3_site_replication_remove_v2::AUTH,
                AdminAction::SiteReplicationRemoveAction,
            ),
        ] {
            let mut req = route(operation, auth, None);
            assert_eq!(
                permission(&req).unwrap(),
                Permission::Admin {
                    action: Action::AdminAction(expected),
                    bucket: None,
                }
            );
            req.subject = Some(&Subject::Caller);
            assert_eq!(permission(&req).unwrap_err().code(), &S3ErrorCode::AccessDenied);
        }

        let mut permissions = route(ops::post_v4_account_permissions::NAME, ops::post_v4_account_permissions::AUTH, None);
        assert_eq!(permission(&permissions).unwrap_err().code(), &S3ErrorCode::AccessDenied);
        permissions.subject = Some(&Subject::Caller);
        assert_eq!(permission(&permissions).unwrap(), Permission::Caller);

        for (unmounted, auth) in [
            (ops::get_v3_bucket_etag_by_bucket::NAME, ops::get_v3_bucket_etag_by_bucket::AUTH),
            (ops::get_v4_heal_mrf_cluster::NAME, ops::get_v4_heal_mrf_cluster::AUTH),
        ] {
            assert_eq!(permission(&route(unmounted, auth, None)).unwrap_err().code(), &S3ErrorCode::InternalError);
        }
    }
}
