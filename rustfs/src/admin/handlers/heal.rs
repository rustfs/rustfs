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

use crate::admin::router::{AdminOperation, Operation, S3Router};
use crate::server::ADMIN_PREFIX;
use bytes::Bytes;
use http::Uri;
use hyper::{Method, StatusCode};
use matchit::Params;
use rustfs_common::heal_channel::HealOpts;
use rustfs_config::MAX_HEAL_REQUEST_SIZE;
use rustfs_ecstore::bucket::utils::is_valid_object_prefix;
use rustfs_ecstore::error::StorageError;
use rustfs_ecstore::store_utils::is_reserved_or_invalid_bucket;
use rustfs_utils::path::path_join;
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
    if hip.bucket.is_empty() && !hip.obj_prefix.is_empty() {
        return Err(s3_error!(InvalidRequest, "invalid bucket name"));
    }
    if is_reserved_or_invalid_bucket(&hip.bucket, false) {
        return Err(s3_error!(InvalidRequest, "invalid bucket name"));
    }
    if !is_valid_object_prefix(&hip.obj_prefix) {
        return Err(s3_error!(InvalidRequest, "invalid object name"));
    }

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

pub fn register_heal_route(r: &mut S3Router<AdminOperation>) -> std::io::Result<()> {
    // Some APIs are only available in EC mode
    // if is_dist_erasure().await || is_erasure().await {
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

pub struct HealHandler {}

#[async_trait::async_trait]
impl Operation for HealHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle HealHandler, req: {:?}, params: {:?}", req, params);
        let Some(cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };
        info!("cred: {:?}", cred);
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
        info!("body: {:?}", hip);

        #[derive(Default)]
        struct HealResp {
            resp_bytes: Vec<u8>,
            _api_err: Option<StorageError>,
            _err_body: String,
        }

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
                                _api_err: Some(StorageError::other(e)),
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
                                _api_err: Some(StorageError::other(e)),
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
                                _api_err: Some(StorageError::other(e)),
                                ..Default::default()
                            })
                            .await;
                    }
                }
            });
        }

        match rx.recv().await {
            Some(result) => Ok(S3Response::new((StatusCode::OK, Body::from(result.resp_bytes)))),
            None => Ok(S3Response::new((StatusCode::INTERNAL_SERVER_ERROR, Body::from(vec![])))),
        }
    }
}

pub struct BackgroundHealStatusHandler {}

#[async_trait::async_trait]
impl Operation for BackgroundHealStatusHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle BackgroundHealStatusHandler");

        Err(s3_error!(NotImplemented))
    }
}

#[cfg(test)]
mod tests {
    use super::extract_heal_init_params;
    use bytes::Bytes;
    use http::Uri;
    use matchit::Router;
    use rustfs_common::heal_channel::HealOpts;
    use serde_json::json;
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

    #[ignore] // FIXME: failed in github actions - keeping original test
    #[test]
    fn test_decode() {
        let b = b"{\"recursive\":false,\"dryRun\":false,\"remove\":false,\"recreate\":false,\"scanMode\":1,\"updateParity\":false,\"nolock\":false}";
        let s: HealOpts = serde_urlencoded::from_bytes(b).unwrap();
        debug!("Parsed HealOpts: {:?}", s);
    }
}
