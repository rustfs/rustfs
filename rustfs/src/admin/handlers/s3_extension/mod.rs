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

//! The S3-shaped extension routes RustFS serves beside the S3 API: replication
//! metrics, validation and resync on `/{bucket}?replication-*`, and the live
//! notification stream on `/?events` and `/{bucket}?events`.
//!
//! Responsible for: recognising these requests, the admin route-table key each
//! one is registered and looked up under, their registration, and the admission
//! every route runs before its handler (identity, license, policy).
//!
//! Not responsible for: object lambda (`/{bucket}/{key}?lambdaArn`), which
//! `admin::router` still serves; the request signature, which the edge verifies
//! before dispatch; any s3s type.
//!
//! Upstream: `admin::router` claims a request with [`route_key`] and dispatches
//! it through the admin route table like every other admin operation.
//! Downstream: `replication`, `listen`.

mod listen;
mod replication;

use crate::admin::auth::authenticate_envelope_credentials;
use crate::admin::operation::AdminRequest;
use crate::admin::route_table;
use crate::admin::router::{AdminOperation, S3Router};
use crate::admin::runtime_sources::current_region;
use crate::admin::storage_api::access::{ReqInfo, authorize_owned_request};
use crate::admin::utils::query_value_exact;
use crate::license::license_check;
use http::{Method, Uri};
use listen::ListenNotification;
use replication::{REPLICATION_EXTENSIONS, ReplicationExtRoute};
use rustfs_policy::policy::action::Action;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
use tracing::error;

const LISTEN_SUB_RESOURCE: &str = "events";

/// The admin route-table key of an extension request: `METHOD|?<sub-resource>/<bucket>`,
/// with an empty bucket for the service-level listen route; `None` when the
/// request is not an extension request.
///
/// An admin path always starts with `/`, so no admin key can collide with one
/// of these. The first matching rule wins, in the order the routes were
/// claimed before they moved here: replication, then listen.
pub(crate) fn route_key(method: &Method, uri: &Uri) -> Option<String> {
    let (sub_resource, bucket) = classify(method, uri)?;
    Some(route_table::route_key(method, &format!("?{sub_resource}/{bucket}")))
}

fn classify<'u>(method: &Method, uri: &'u Uri) -> Option<(&'static str, &'u str)> {
    if let Some(bucket) = bucket_level(uri.path())
        && let Some(route) = ReplicationExtRoute::classify(method, uri)
    {
        return Some((route.sub_resource(), bucket));
    }
    if method != Method::GET || query_value_exact(uri, LISTEN_SUB_RESOURCE).is_none() {
        return None;
    }
    let path = uri.path();
    if path == "/" {
        return Some((LISTEN_SUB_RESOURCE, ""));
    }
    bucket_level(path.strip_suffix('/').unwrap_or(path)).map(|bucket| (LISTEN_SUB_RESOURCE, bucket))
}

fn bucket_level(path: &str) -> Option<&str> {
    let bucket = path.strip_prefix('/')?;
    (!bucket.is_empty() && !bucket.contains('/')).then_some(bucket)
}

pub(crate) fn register_s3_extension_routes(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    for handler in &REPLICATION_EXTENSIONS {
        let route = handler.route();
        r.insert(route.method(), &format!("?{}/{{bucket}}", route.sub_resource()), AdminOperation(handler))?;
    }
    r.insert(Method::GET, &format!("?{LISTEN_SUB_RESOURCE}/"), AdminOperation(&ListenNotification))?;
    r.insert(
        Method::GET,
        &format!("?{LISTEN_SUB_RESOURCE}/{{bucket}}"),
        AdminOperation(&ListenNotification),
    )
}

/// Admits a caller whose signature the edge already verified: resolves its
/// identity, records the request info the policy decision reads, checks the
/// license, then authorizes `action` on `bucket`.
async fn authorize(req: &mut AdminRequest, bucket: Option<&str>, action: Action) -> S3Result<()> {
    let Some(credentials) = req.credentials() else {
        return Err(S3Error::with_message(S3ErrorCode::AccessDenied, "Signature is required"));
    };
    let (cred, is_owner) = authenticate_envelope_credentials(credentials).await?;
    req.install_req_info(ReqInfo {
        cred: Some(cred),
        is_owner,
        bucket: bucket.map(str::to_owned),
        region: current_region(),
        ..Default::default()
    });
    license_check().map_err(|er| match er.kind() {
        std::io::ErrorKind::PermissionDenied => S3Error::with_message(S3ErrorCode::AccessDenied, format!("{er}")),
        _ => {
            error!(
                event = "admin_router_state",
                component = "admin",
                subsystem = "s3_extension",
                result = "license_check_failed",
                error = %er,
                "admin router state"
            );
            S3Error::with_message(S3ErrorCode::InternalError, "License validation failed")
        }
    })?;
    authorize_owned_request(req.envelope_mut(), action, None).await
}

#[cfg(test)]
mod tests {
    use super::route_key;
    use http::{Method, Uri};

    fn key(method: Method, uri: &str) -> Option<String> {
        route_key(&method, &uri.parse::<Uri>().expect("test uri parses"))
    }

    #[test]
    fn route_key_names_each_route_and_its_bucket() {
        for (method, uri, expected) in [
            (Method::PUT, "/demo-bucket?replication-reset", "PUT|?replication-reset/demo-bucket"),
            (
                Method::GET,
                "/demo-bucket?replication-reset-status",
                "GET|?replication-reset-status/demo-bucket",
            ),
            (
                Method::GET,
                "/demo-bucket?replication-metrics=2",
                "GET|?replication-metrics=2/demo-bucket",
            ),
            (Method::GET, "/demo-bucket?replication-metrics", "GET|?replication-metrics/demo-bucket"),
            (Method::GET, "/demo-bucket?replication-metrics=", "GET|?replication-metrics/demo-bucket"),
            (Method::GET, "/demo-bucket?replication-check", "GET|?replication-check/demo-bucket"),
            (Method::GET, "/?events=s3:ObjectRemoved:*", "GET|?events/"),
            (Method::GET, "/demo-bucket?events=s3:ObjectCreated:*", "GET|?events/demo-bucket"),
            (Method::GET, "/demo-bucket/?events=s3:ObjectCreated:*", "GET|?events/demo-bucket"),
        ] {
            assert_eq!(key(method.clone(), uri).as_deref(), Some(expected), "{method} {uri}");
        }
    }

    #[test]
    fn route_key_keeps_the_claim_order_between_overlapping_sub_resources() {
        for (method, uri, expected) in [
            (
                Method::GET,
                "/b?replication-check&replication-reset-status",
                "GET|?replication-reset-status/b",
            ),
            (Method::GET, "/b?replication-check&replication-metrics=2", "GET|?replication-metrics=2/b"),
            (Method::GET, "/b?replication-metrics=3&replication-check", "GET|?replication-check/b"),
            (Method::GET, "/b?events=x&replication-check", "GET|?replication-check/b"),
            (Method::GET, "/b/?events=x&replication-check", "GET|?events/b"),
            (Method::GET, "/?replication-check&events=x", "GET|?events/"),
        ] {
            assert_eq!(key(method.clone(), uri).as_deref(), Some(expected), "{method} {uri}");
        }
    }

    #[test]
    fn route_key_rejects_other_paths_methods_and_values() {
        for (method, uri) in [
            (Method::GET, "/demo-bucket/path/file?replication-metrics"),
            (Method::GET, "/demo-bucket/?replication-metrics"),
            (Method::GET, "/demo-bucket?replication-metrics=1"),
            (Method::GET, "/demo-bucket?replication-check=1"),
            (Method::GET, "/demo-bucket?replication-reset-status=x"),
            (Method::PUT, "/demo-bucket?replication-reset=1"),
            (Method::PUT, "/demo-bucket?replication-check"),
            (Method::GET, "/demo-bucket?replication-reset"),
            (Method::PUT, "/demo-bucket?replication-reset-status"),
            (Method::POST, "/demo-bucket?replication-metrics"),
            (Method::GET, "/?replication-metrics"),
            (Method::GET, "/demo-bucket/object?events=s3:ObjectCreated:*"),
            (Method::GET, "//?events=s3:ObjectCreated:*"),
            (Method::PUT, "/demo-bucket?events=s3:ObjectCreated:*"),
            (Method::GET, "/demo-bucket?event=s3:ObjectCreated:*"),
            (Method::GET, "/demo-bucket/object?lambdaArn=arn%3Atarget"),
            (Method::GET, "/rustfs/admin/v3/info"),
        ] {
            assert_eq!(key(method.clone(), uri), None, "{method} {uri}");
        }
    }
}
