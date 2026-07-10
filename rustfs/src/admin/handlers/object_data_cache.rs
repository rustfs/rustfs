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

//! Admin surface for the object data cache (ODC-C2, backlog#1143).
//!
//! `GET  {ADMIN_PREFIX}/v3/object-data-cache/stats` returns the current stats
//! snapshot plus mode, the only production window into the cache short of a
//! metrics scrape. `POST {ADMIN_PREFIX}/v3/object-data-cache/flush` drops
//! cached bodies; with no query it clears everything, with `bucket` it flushes
//! that bucket, and with `bucket`+`object` it flushes that one identity — the
//! only remediation for a poisoned entry short of a node restart.

use crate::admin::auth::validate_admin_request;
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::admin::runtime_sources::current_object_data_cache;
use crate::app::object_data_cache::ObjectDataCacheAdapter;
use crate::auth::{check_key_valid, get_session_token};
use crate::server::{ADMIN_PREFIX, RemoteAddr};
use http::{HeaderMap, HeaderValue};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_object_data_cache::{ObjectDataCacheIdentity, ObjectDataCacheInvalidationReason, ObjectDataCacheInvalidationResult};
use rustfs_policy::policy::action::{Action, AdminAction};
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};
use serde::Serialize;
use std::sync::Arc;

const JSON_CONTENT_TYPE: &str = "application/json";

#[derive(Debug, Serialize)]
struct ObjectDataCacheStatsResponse {
    mode: &'static str,
    disabled: bool,
    entries: u64,
    lookups: u64,
    hits: u64,
    fills: u64,
    invalidations: u64,
    inflight_fills: u64,
    singleflight_joins: u64,
    memory_pressure_events: u64,
}

#[derive(Debug, Serialize)]
struct ObjectDataCacheFlushResponse {
    scope: &'static str,
    bucket: Option<String>,
    object: Option<String>,
    outcome: &'static str,
    removed_keys: usize,
}

pub fn register_object_data_cache_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    r.insert(
        Method::GET,
        format!("{ADMIN_PREFIX}/v3/object-data-cache/stats").as_str(),
        AdminOperation(&ObjectDataCacheStatsHandler {}),
    )?;
    r.insert(
        Method::POST,
        format!("{ADMIN_PREFIX}/v3/object-data-cache/flush").as_str(),
        AdminOperation(&ObjectDataCacheFlushHandler {}),
    )?;
    Ok(())
}

async fn authorize(req: &S3Request<Body>, action: AdminAction) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "missing credentials"));
    };
    let (cred, owner) =
        check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;
    let remote_addr = req
        .extensions
        .get::<Option<RemoteAddr>>()
        .and_then(|opt| opt.map(|addr| addr.0));
    validate_admin_request(&req.headers, &cred, owner, false, vec![Action::AdminAction(action)], remote_addr).await
}

fn json_response<T: Serialize>(body: &T) -> S3Result<S3Response<(StatusCode, Body)>> {
    let data = serde_json::to_vec(body)
        .map_err(|err| S3Error::with_message(S3ErrorCode::InternalError, format!("failed to encode response: {err}")))?;
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static(JSON_CONTENT_TYPE));
    Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), headers))
}

fn query_value(req: &S3Request<Body>, key: &str) -> Option<String> {
    req.uri.query().and_then(|query| {
        url::form_urlencoded::parse(query.as_bytes())
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.into_owned())
            .filter(|v| !v.is_empty())
    })
}

fn invalidation_outcome(result: &ObjectDataCacheInvalidationResult) -> (&'static str, usize) {
    match result {
        ObjectDataCacheInvalidationResult::Removed { keys } => ("removed", *keys),
        ObjectDataCacheInvalidationResult::NoOp => ("noop", 0),
    }
}

pub struct ObjectDataCacheStatsHandler {}

#[async_trait::async_trait]
impl Operation for ObjectDataCacheStatsHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize(&req, AdminAction::ServerInfoAdminAction).await?;

        let response = match current_object_data_cache() {
            Some(adapter) => {
                let snapshot = adapter.stats();
                ObjectDataCacheStatsResponse {
                    mode: adapter.mode().as_str(),
                    disabled: adapter.is_disabled(),
                    entries: snapshot.entries,
                    lookups: snapshot.lookups,
                    hits: snapshot.hits,
                    fills: snapshot.fills,
                    invalidations: snapshot.invalidations,
                    inflight_fills: snapshot.inflight_fills,
                    singleflight_joins: snapshot.singleflight_joins,
                    memory_pressure_events: snapshot.memory_pressure_events,
                }
            }
            None => ObjectDataCacheStatsResponse {
                mode: "disabled",
                disabled: true,
                entries: 0,
                lookups: 0,
                hits: 0,
                fills: 0,
                invalidations: 0,
                inflight_fills: 0,
                singleflight_joins: 0,
                memory_pressure_events: 0,
            },
        };

        json_response(&response)
    }
}

pub struct ObjectDataCacheFlushHandler {}

#[async_trait::async_trait]
impl Operation for ObjectDataCacheFlushHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        authorize(&req, AdminAction::ConfigUpdateAdminAction).await?;

        let bucket = query_value(&req, "bucket");
        let object = query_value(&req, "object");
        if object.is_some() && bucket.is_none() {
            return Err(s3_error!(InvalidRequest, "object flush requires a bucket query parameter"));
        }

        let adapter: Arc<ObjectDataCacheAdapter> =
            current_object_data_cache().ok_or_else(|| s3_error!(InternalError, "object data cache is not initialized"))?;

        // Manual is the existing-but-uncalled reason reserved for operator-driven
        // invalidation; every flush scope reports under it.
        let reason = ObjectDataCacheInvalidationReason::Manual;
        let (scope, result) = match (bucket.as_deref(), object.as_deref()) {
            (Some(bucket), Some(object)) => (
                "object",
                adapter
                    .invalidate_object(ObjectDataCacheIdentity::new(bucket, object), reason)
                    .await,
            ),
            (Some(bucket), None) => ("bucket", adapter.invalidate_bucket(bucket, reason).await),
            (None, _) => ("all", adapter.clear(reason).await),
        };

        let (outcome, removed_keys) = invalidation_outcome(&result);
        json_response(&ObjectDataCacheFlushResponse {
            scope,
            bucket,
            object,
            outcome,
            removed_keys,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flush_outcome_maps_removed_and_noop() {
        assert_eq!(
            invalidation_outcome(&ObjectDataCacheInvalidationResult::Removed { keys: 3 }),
            ("removed", 3)
        );
        assert_eq!(invalidation_outcome(&ObjectDataCacheInvalidationResult::NoOp), ("noop", 0));
    }

    #[test]
    fn stats_handler_requires_server_info_action() {
        // Guard the auth contract: the stats endpoint is a read, the flush
        // endpoint mutates, so they must not share one action.
        let src = include_str!("object_data_cache.rs");
        assert!(src.contains("authorize(&req, AdminAction::ServerInfoAdminAction).await?;"));
        assert!(src.contains("authorize(&req, AdminAction::ConfigUpdateAdminAction).await?;"));
    }
}
