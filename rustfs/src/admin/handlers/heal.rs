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

use crate::admin::auth::{authenticate_request, validate_admin_request};
use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::server::ADMIN_PREFIX;
use crate::server::RemoteAddr;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Uri};
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_common::heal_channel::HealOpts;
use rustfs_config::MAX_HEAL_REQUEST_SIZE;
use rustfs_ecstore::bucket::utils::is_valid_object_prefix;
use rustfs_ecstore::new_object_layer_fn;
use rustfs_ecstore::store_api::HealOperations;
use rustfs_ecstore::store_utils::is_reserved_or_invalid_bucket;
use rustfs_policy::policy::action::{Action, AdminAction};
use rustfs_scanner::scanner::{BackgroundHealInfo, read_background_heal_info};
use rustfs_utils::path::path_join;
use s3s::header::CONTENT_TYPE;
use s3s::{Body, S3Request, S3Response, S3Result, s3_error};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::spawn;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Debug, Default, Serialize, Deserialize)]
struct HealInitParams {
    bucket: String,
    obj_prefix: String,
    hs: HealOpts,
    client_token: String,
    force_start: bool,
    force_stop: bool,
}

fn extract_heal_init_params(body: &Bytes, uri: &Uri, params: Params<'_, '_>) -> S3Result<HealInitParams> {
    let mut hip = HealInitParams {
        bucket: params.get("bucket").map(|s| s.to_string()).unwrap_or_default(),
        obj_prefix: params.get("prefix").map(|s| s.to_string()).unwrap_or_default(),
        ..Default::default()
    };
    validate_heal_target(&hip.bucket, &hip.obj_prefix)?;

    if let Some(query) = uri.query() {
        let params: Vec<&str> = query.split('&').collect();
        for param in params {
            let mut parts = param.split('=');
            if let Some(key) = parts.next() {
                if key == "clientToken"
                    && let Some(value) = parts.next()
                {
                    hip.client_token = value.to_string();
                }
                if key == "forceStart" && parts.next().is_some() {
                    hip.force_start = true;
                }
                if key == "forceStop" && parts.next().is_some() {
                    hip.force_stop = true;
                }
            }
        }
    }

    if (hip.force_start && hip.force_stop) || (!hip.client_token.is_empty() && (hip.force_start || hip.force_stop)) {
        return Err(s3_error!(
            InvalidRequest,
            "invalid combination of clientToken, forceStart, and forceStop parameters"
        ));
    }

    if hip.client_token.is_empty() {
        hip.hs = serde_json::from_slice(body).map_err(|e| {
            info!("err request body parse, err: {:?}", e);
            s3_error!(InvalidRequest, "err request body parse")
        })?;
    }

    Ok(hip)
}

fn validate_heal_target(bucket: &str, obj_prefix: &str) -> S3Result<()> {
    if bucket.is_empty() && !obj_prefix.is_empty() {
        return Err(s3_error!(InvalidRequest, "invalid bucket name"));
    }
    if !bucket.is_empty() && is_reserved_or_invalid_bucket(bucket, false) {
        return Err(s3_error!(InvalidRequest, "invalid bucket name"));
    }
    if !is_valid_object_prefix(obj_prefix) {
        return Err(s3_error!(InvalidRequest, "invalid object name"));
    }

    Ok(())
}

pub fn register_heal_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    // Some APIs are only available in EC mode
    // if is_dist_erasure().await || is_erasure().await {
    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/").as_str(),
        AdminOperation(&HealHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/{bucket}").as_str(),
        AdminOperation(&HealHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/heal/{bucket}/{prefix}").as_str(),
        AdminOperation(&HealHandler {}),
    )?;

    r.insert(
        Method::POST,
        format!("{}{}", ADMIN_PREFIX, "/v3/background-heal/status").as_str(),
        AdminOperation(&BackgroundHealStatusHandler {}),
    )?;

    Ok(())
}

#[derive(Default)]
struct HealResp {
    resp_bytes: Vec<u8>,
    api_err: Option<String>,
}

fn map_heal_response(result: Option<HealResp>) -> S3Result<(StatusCode, Vec<u8>)> {
    match result {
        Some(result) => {
            if let Some(err) = result.api_err {
                return Err(s3_error!(InternalError, "{err}"));
            }

            Ok((StatusCode::OK, result.resp_bytes))
        }
        None => Err(s3_error!(InternalError, "heal channel closed unexpectedly")),
    }
}

fn encode_background_heal_status(info: &BackgroundHealInfo) -> S3Result<Vec<u8>> {
    serde_json::to_vec(info).map_err(|e| s3_error!(InternalError, "failed to serialize background heal status: {e}"))
}

fn validate_heal_request_mode(hip: &HealInitParams) -> S3Result<()> {
    if hip.bucket.is_empty() && hip.client_token.is_empty() && !hip.force_stop {
        return Err(s3_error!(InvalidRequest, "starting heal without a bucket target is not supported"));
    }

    Ok(())
}

fn should_handle_root_heal_directly(hip: &HealInitParams) -> bool {
    hip.bucket.is_empty() && hip.obj_prefix.is_empty() && hip.client_token.is_empty() && !hip.force_stop
}

fn map_root_heal_status(heal_err: Option<rustfs_ecstore::error::Error>) -> S3Result<()> {
    match heal_err {
        None => Ok(()),
        Some(rustfs_ecstore::error::StorageError::NoHealRequired) => {
            warn!("root heal completed with non-fatal status: no heal required");
            Ok(())
        }
        Some(err) => Err(s3_error!(InternalError, "root heal failed: {err}")),
    }
}

fn json_response(status: StatusCode, body: Vec<u8>) -> S3Response<(StatusCode, Body)> {
    let mut headers = HeaderMap::new();
    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    S3Response::with_headers((status, Body::from(body)), headers)
}

async fn validate_heal_admin_request(req: &S3Request<Body>) -> S3Result<()> {
    let Some(input_cred) = req.credentials.as_ref() else {
        return Err(s3_error!(InvalidRequest, "authentication required"));
    };

    let (cred, owner) = authenticate_request(&req.headers, &req.uri, input_cred).await?;

    validate_admin_request(
        &req.headers,
        &cred,
        owner,
        false,
        vec![Action::AdminAction(AdminAction::HealAdminAction)],
        req.extensions.get::<Option<RemoteAddr>>().and_then(|opt| opt.map(|a| a.0)),
    )
    .await
}

pub struct HealHandler {}

#[async_trait::async_trait]
impl Operation for HealHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle HealHandler, req: {:?}, params: {:?}", req, params);
        validate_heal_admin_request(&req).await?;
        let mut input = req.input;
        let bytes = match input.store_all_limited(MAX_HEAL_REQUEST_SIZE).await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "heal request body too large or failed to read"));
            }
        };
        info!("bytes: {:?}", bytes);
        let hip = extract_heal_init_params(&bytes, &req.uri, params)?;
        // The heal channel currently models bucket/object work. Root heal reuses the
        // existing format-heal path directly so `/v3/heal/` is accepted intentionally.
        if should_handle_root_heal_directly(&hip) {
            let Some(store) = new_object_layer_fn() else {
                return Err(s3_error!(InternalError, "server not initialized"));
            };

            let (_, heal_err) = store
                .heal_format(hip.hs.dry_run)
                .await
                .map_err(|e| s3_error!(InternalError, "root heal failed: {e}"))?;

            map_root_heal_status(heal_err)?;

            return Ok(S3Response::new((StatusCode::OK, Body::empty())));
        }
        validate_heal_request_mode(&hip)?;
        info!("body: {:?}", hip);

        let heal_path = path_join(&[PathBuf::from(hip.bucket.clone()), PathBuf::from(hip.obj_prefix.clone())]);
        let (tx, mut rx) = mpsc::channel(1);

        if !hip.client_token.is_empty() && !hip.force_start && !hip.force_stop {
            // Query heal status
            let tx_clone = tx.clone();
            let heal_path_str = heal_path.to_str().unwrap_or_default().to_string();
            let client_token = hip.client_token.clone();
            spawn(async move {
                match rustfs_common::heal_channel::query_heal_status(heal_path_str, client_token).await {
                    Ok(_) => {
                        // TODO: Get actual response from channel
                        let _ = tx_clone
                            .send(HealResp {
                                resp_bytes: vec![],
                                ..Default::default()
                            })
                            .await;
                    }
                    Err(e) => {
                        let _ = tx_clone
                            .send(HealResp {
                                api_err: Some(format!("query heal status failed: {e}")),
                                ..Default::default()
                            })
                            .await;
                    }
                }
            });
        } else if hip.force_stop {
            // Cancel heal task
            let tx_clone = tx.clone();
            let heal_path_str = heal_path.to_str().unwrap_or_default().to_string();
            spawn(async move {
                match rustfs_common::heal_channel::cancel_heal_task(heal_path_str).await {
                    Ok(_) => {
                        // TODO: Get actual response from channel
                        let _ = tx_clone
                            .send(HealResp {
                                resp_bytes: vec![],
                                ..Default::default()
                            })
                            .await;
                    }
                    Err(e) => {
                        let _ = tx_clone
                            .send(HealResp {
                                api_err: Some(format!("cancel heal task failed: {e}")),
                                ..Default::default()
                            })
                            .await;
                    }
                }
            });
        } else if hip.client_token.is_empty() {
            // Use new heal channel mechanism
            let tx_clone = tx.clone();
            spawn(async move {
                // Create heal request through channel
                let heal_request = rustfs_common::heal_channel::create_heal_request(
                    hip.bucket.clone(),
                    if hip.obj_prefix.is_empty() {
                        None
                    } else {
                        Some(hip.obj_prefix.clone())
                    },
                    hip.force_start,
                    Some(rustfs_common::heal_channel::HealChannelPriority::Normal),
                );

                match rustfs_common::heal_channel::send_heal_request(heal_request).await {
                    Ok(_) => {
                        // Success - send empty response for now
                        let _ = tx_clone
                            .send(HealResp {
                                resp_bytes: vec![],
                                ..Default::default()
                            })
                            .await;
                    }
                    Err(e) => {
                        // Error - send error response
                        let _ = tx_clone
                            .send(HealResp {
                                api_err: Some(format!("send heal request failed: {e}")),
                                ..Default::default()
                            })
                            .await;
                    }
                }
            });
        }

        let (status, body) = map_heal_response(rx.recv().await)?;
        Ok(S3Response::new((status, Body::from(body))))
    }
}

pub struct BackgroundHealStatusHandler {}

#[async_trait::async_trait]
impl Operation for BackgroundHealStatusHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle BackgroundHealStatusHandler");
        validate_heal_admin_request(&req).await?;

        let Some(store) = new_object_layer_fn() else {
            return Err(s3_error!(InternalError, "server not initialized"));
        };

        let info = read_background_heal_info(store).await;
        let body = encode_background_heal_status(&info)?;

        Ok(json_response(StatusCode::OK, body))
    }
}

#[cfg(test)]
mod tests {
    use super::extract_heal_init_params;
    use super::{
        HealInitParams, HealResp, encode_background_heal_status, json_response, map_heal_response, map_root_heal_status,
        should_handle_root_heal_directly, validate_heal_request_mode, validate_heal_target,
    };
    use bytes::Bytes;
    use http::StatusCode;
    use http::Uri;
    use matchit::Router;
    use rustfs_common::heal_channel::{HealOpts, HealScanMode};
    use rustfs_ecstore::error::StorageError;
    use rustfs_scanner::scanner::BackgroundHealInfo;
    use s3s::{S3ErrorCode, header::CONTENT_TYPE};
    use serde_json::json;
    use tokio::sync::mpsc;
    use tracing::debug;

    #[test]
    fn test_heal_opts_serialization() {
        // Test that HealOpts can be properly deserialized
        let heal_opts_json = json!({
            "recursive": true,
            "dryRun": false,
            "remove": true,
            "recreate": false,
            "scanMode": 2,
            "updateParity": true,
            "nolock": false
        });

        let json_str = serde_json::to_string(&heal_opts_json).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json_str).unwrap();

        assert_eq!(parsed["recursive"], true);
        assert_eq!(parsed["scanMode"], 2);
    }

    #[test]
    fn test_heal_opts_url_encoding() {
        // Test URL encoding/decoding of HealOpts
        let opts = HealOpts {
            recursive: true,
            dry_run: false,
            remove: true,
            recreate: false,
            scan_mode: rustfs_common::heal_channel::HealScanMode::Normal,
            update_parity: false,
            no_lock: true,
            pool: Some(1),
            set: Some(0),
        };

        let encoded = serde_urlencoded::to_string(opts).unwrap();
        assert!(encoded.contains("recursive=true"));
        assert!(encoded.contains("remove=true"));

        // Test round-trip
        let decoded: HealOpts = serde_urlencoded::from_str(&encoded).unwrap();
        assert_eq!(decoded.recursive, opts.recursive);
        assert_eq!(decoded.scan_mode, opts.scan_mode);
    }

    #[test]
    fn test_extract_heal_init_params_invalid_control_combination_returns_descriptive_error() {
        let uri: Uri = "/rustfs/admin/v3/heal/test-bucket?clientToken=token&forceStart=true"
            .parse()
            .expect("uri should parse");

        let mut router = Router::new();
        router
            .insert("/rustfs/admin/v3/heal/{bucket}", ())
            .expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/heal/test-bucket").expect("route should match");

        let err = extract_heal_init_params(&Bytes::new(), &uri, matched.params).expect_err("must reject invalid combo");
        assert!(
            err.to_string()
                .contains("invalid combination of clientToken, forceStart, and forceStop parameters")
        );
    }

    #[test]
    fn test_extract_heal_init_params_allows_root_heal_target() {
        let uri: Uri = "/rustfs/admin/v3/heal/".parse().expect("uri should parse");
        let heal_opts = json!({
            "recursive": false,
            "dryRun": false,
            "remove": false,
            "recreate": false,
            "scanMode": 1,
            "updateParity": false,
            "nolock": false
        });

        let mut router = Router::new();
        router.insert("/rustfs/admin/v3/heal/", ()).expect("route should insert");
        let matched = router.at("/rustfs/admin/v3/heal/").expect("route should match");

        let parsed = extract_heal_init_params(
            &Bytes::from(serde_json::to_vec(&heal_opts).expect("json should serialize")),
            &uri,
            matched.params,
        )
        .expect("root heal target should be accepted");

        assert!(parsed.bucket.is_empty());
        assert!(parsed.obj_prefix.is_empty());
    }

    #[test]
    fn test_validate_heal_request_mode_rejects_root_heal_start() {
        let err = validate_heal_request_mode(&HealInitParams::default()).expect_err("must reject root heal start");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(
            err.to_string()
                .contains("starting heal without a bucket target is not supported")
        );
    }

    #[test]
    fn test_should_handle_root_heal_directly_for_root_start_modes() {
        assert!(should_handle_root_heal_directly(&HealInitParams::default()));
        assert!(should_handle_root_heal_directly(&HealInitParams {
            force_start: true,
            ..Default::default()
        }));
    }

    #[test]
    fn test_should_handle_root_heal_directly_skips_query_cancel_and_bucket_targets() {
        assert!(!should_handle_root_heal_directly(&HealInitParams {
            client_token: "heal-token".to_string(),
            ..Default::default()
        }));
        assert!(!should_handle_root_heal_directly(&HealInitParams {
            force_stop: true,
            ..Default::default()
        }));
        assert!(!should_handle_root_heal_directly(&HealInitParams {
            bucket: "bucket".to_string(),
            ..Default::default()
        }));
    }

    #[test]
    fn test_map_root_heal_status_allows_no_heal_required() {
        map_root_heal_status(Some(StorageError::NoHealRequired)).expect("NoHealRequired should stay non-fatal");
    }

    #[test]
    fn test_map_root_heal_status_rejects_fatal_errors() {
        let err = map_root_heal_status(Some(StorageError::Unexpected)).expect_err("fatal status must fail");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
        assert!(err.to_string().contains("root heal failed: Unexpected error"));
    }

    #[test]
    fn test_validate_heal_request_mode_allows_root_query_and_cancel() {
        validate_heal_request_mode(&HealInitParams {
            client_token: "heal-token".to_string(),
            ..Default::default()
        })
        .expect("root heal status query should be accepted");

        validate_heal_request_mode(&HealInitParams {
            force_stop: true,
            ..Default::default()
        })
        .expect("root heal cancel should be accepted");
    }

    #[test]
    fn test_extract_heal_init_params_rejects_prefix_without_bucket() {
        let err = validate_heal_target("", "prefix").expect_err("must reject empty bucket");
        assert_eq!(err.code(), &S3ErrorCode::InvalidRequest);
        assert!(err.to_string().contains("invalid bucket name"));
    }

    #[ignore] // FIXME: failed in github actions - keeping original test
    #[test]
    fn test_decode() {
        let b = b"{\"recursive\":false,\"dryRun\":false,\"remove\":false,\"recreate\":false,\"scanMode\":1,\"updateParity\":false,\"nolock\":false}";
        let s: HealOpts = serde_urlencoded::from_bytes(b).unwrap();
        debug!("Parsed HealOpts: {:?}", s);
    }

    #[tokio::test]
    async fn test_map_heal_response_propagates_errors() {
        let (tx, mut rx) = mpsc::channel(1);
        drop(tx);
        let result = map_heal_response(rx.recv().await);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), &S3ErrorCode::InternalError);

        let (tx, mut rx) = mpsc::channel(1);
        tx.send(HealResp {
            resp_bytes: vec![],
            api_err: Some("channel failed".to_string()),
        })
        .await
        .unwrap();
        let result = map_heal_response(rx.recv().await);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().code(), &S3ErrorCode::InternalError);

        let (tx, mut rx) = mpsc::channel(1);
        let _ = tx
            .send(HealResp {
                resp_bytes: vec![1, 2, 3],
                api_err: None,
            })
            .await;
        let result = map_heal_response(rx.recv().await).expect("heal response should be successful");
        assert_eq!(result.0, StatusCode::OK);
        assert_eq!(result.1, vec![1, 2, 3]);
    }

    #[test]
    fn test_encode_background_heal_status_uses_expected_shape() {
        let info = BackgroundHealInfo {
            bitrot_start_time: None,
            bitrot_start_cycle: 42,
            current_scan_mode: HealScanMode::Deep,
        };

        let encoded = encode_background_heal_status(&info).expect("background heal info should serialize");
        let json: serde_json::Value = serde_json::from_slice(&encoded).expect("json should deserialize");

        assert_eq!(json["bitrotStartCycle"], 42);
        assert_eq!(json["currentScanMode"], 2);
        assert!(json["bitrotStartTime"].is_null());
    }

    #[test]
    fn test_json_response_sets_application_json_content_type() {
        let response = json_response(StatusCode::OK, b"{}".to_vec());
        let content_type = response.headers.get(CONTENT_TYPE).and_then(|value| value.to_str().ok());
        assert_eq!(content_type, Some("application/json"),);
    }
}
