use super::router::Operation;
use crate::storage::error::to_s3_error;
use bytes::Bytes;
use ecstore::bucket::policy::action::{Action, ActionSet};
use ecstore::bucket::policy::bucket_policy::{BPStatement, BucketPolicy};
use ecstore::bucket::policy::effect::Effect;
use ecstore::bucket::policy::resource::{Resource, ResourceSet};
use ecstore::error::Error as ec_Error;
use ecstore::global::GLOBAL_ALlHealState;
use ecstore::heal::heal_commands::HealOpts;
use ecstore::heal::heal_ops::new_heal_sequence;
use ecstore::peer::is_reserved_or_invalid_bucket;
use ecstore::store::is_valid_object_prefix;
use ecstore::store_api::StorageAPI;
use ecstore::utils::path::path_join;
use ecstore::utils::xml;
use ecstore::GLOBAL_Endpoints;
use ecstore::{new_object_layer_fn, store_api::BackendInfo};
use http::Uri;
use hyper::StatusCode;
use matchit::Params;
use s3s::S3ErrorCode;
use s3s::{
    dto::{AssumeRoleOutput, Credentials, Timestamp},
    s3_error, Body, S3Error, S3Request, S3Response, S3Result,
};
use serde::{Deserialize, Serialize};
use serde_urlencoded::from_bytes;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use time::{Duration, OffsetDateTime};
use tokio::spawn;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[derive(Deserialize, Debug, Default)]
#[serde(rename_all = "PascalCase", default)]
pub struct AssumeRoleRequest {
    pub action: String,
    pub duration_seconds: usize,
    pub version: String,
    pub role_arn: String,
    pub role_session_name: String,
    pub policy: String,
    pub external_id: String,
}

// #[derive(Debug, Serialize, Default)]
// #[serde(rename_all = "PascalCase", default)]
// pub struct AssumeRoleResponse {
//     #[serde(rename = "AssumeRoleResult")]
//     pub result: AssumeRoleResult,
// }

// #[derive(Debug, Serialize, Default)]
// #[serde(rename_all = "PascalCase", default)]
// pub struct AssumeRoleResult {
//     pub credentials: Credentials,
// }

// #[derive(Debug, Serialize, Default)]
// #[serde(rename_all = "PascalCase", default)]
// pub struct Credentials {
//     #[serde(rename = "AccessKeyId")]
//     pub access_key: String,
//     #[serde(rename = "SecretAccessKey")]
//     pub secret_key: String,
//     pub status: String,
//     pub expiration: usize,
//     pub session_token: String,
//     pub parent_user: String,
// }

pub struct AssumeRoleHandle {}
#[async_trait::async_trait]
impl Operation for AssumeRoleHandle {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle AssumeRoleHandle");

        let Some(cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };

        let mut input = req.input;

        let Some(bytes) = input.take_bytes() else {
            return Err(s3_error!(InvalidRequest, "get body failed"));
        };
        let body: AssumeRoleRequest = from_bytes(&bytes).map_err(|_e| s3_error!(InvalidRequest, "get body failed"))?;

        warn!("AssumeRole get body {:?}", body);

        let exp = OffsetDateTime::now_utc().saturating_add(Duration::days(1));

        // TODO: create tmp access_key
        let resp = AssumeRoleOutput {
            credentials: Some(Credentials {
                access_key_id: cred.access_key,
                expiration: Timestamp::from(exp),
                secret_access_key: cred.secret_key.expose().to_string(),
                session_token: "sdfsdf".to_owned(),
            }),
            ..Default::default()
        };

        // getAssumeRoleCredentials
        let output = xml::serialize::<AssumeRoleOutput>(&resp).unwrap();

        Ok(S3Response::new((StatusCode::OK, Body::from(output))))

        // return Err(s3_error!(NotImplemented));
    }
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "PascalCase", default)]
pub struct AccountInfo {
    pub account_name: String,
    pub server: BackendInfo,
    pub policy: BucketPolicy,
}

pub struct AccountInfoHandler {}
#[async_trait::async_trait]
impl Operation for AccountInfoHandler {
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle AccountInfoHandler");

        let Some(cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };

        warn!("AccountInfoHandler cread {:?}", &cred);

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // test policy

        let mut s3_all_act = HashSet::with_capacity(1);
        s3_all_act.insert(Action::AllActions);

        let mut all_res = HashSet::with_capacity(1);
        all_res.insert(Resource::new("*"));

        let bucket_policy = BucketPolicy {
            id: "".to_owned(),
            version: "2012-10-17".to_owned(),
            statements: vec![BPStatement {
                sid: "".to_owned(),
                effect: Effect::Allow,
                actions: ActionSet(s3_all_act.clone()),
                resources: ResourceSet(all_res),
                ..Default::default()
            }],
        };

        // let policy = bucket_policy
        //     .marshal_msg()
        //     .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse policy failed"))?;

        let backend_info = store.backend_info().await;

        let info = AccountInfo {
            account_name: cred.access_key,
            server: backend_info,
            policy: bucket_policy,
        };

        let output = serde_json::to_string(&info)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(output))))
    }
}

pub struct ServiceHandle {}
#[async_trait::async_trait]
impl Operation for ServiceHandle {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ServiceHandle");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct ServerInfoHandler {}

#[async_trait::async_trait]
impl Operation for ServerInfoHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ServerInfoHandler");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct InspectDataHandler {}

#[async_trait::async_trait]
impl Operation for InspectDataHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle InspectDataHandler");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct StorageInfoHandler {}

#[async_trait::async_trait]
impl Operation for StorageInfoHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle StorageInfoHandler");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct DataUsageInfoHandler {}

#[async_trait::async_trait]
impl Operation for DataUsageInfoHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle DataUsageInfoHandler");

        return Err(s3_error!(NotImplemented));
    }
}

pub struct MetricsHandler {}

#[async_trait::async_trait]
impl Operation for MetricsHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle MetricsHandler");

        return Err(s3_error!(NotImplemented));
    }
}

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
                if key == "clientToken" {
                    if let Some(value) = parts.next() {
                        hip.client_token = value.to_string();
                    }
                }
                if key == "forceStart" {
                    if parts.next().is_some() {
                        hip.force_start = true;
                    }
                }
                if key == "forceStop" {
                    if parts.next().is_some() {
                        hip.force_stop = true;
                    }
                }
            }
        }
    }

    if (hip.force_start && hip.force_stop) || (!hip.client_token.is_empty() && (hip.force_start || hip.force_stop)) {
        return Err(s3_error!(InvalidRequest, ""));
    }

    if hip.client_token.is_empty() {
        hip.hs = serde_json::from_slice(body).map_err(|e| {
            info!("err request body parse, err: {:?}", e);
            s3_error!(InvalidRequest, "err request body parse")
        })?;
    }

    Ok(hip)
}

pub struct HealHandler {}

#[async_trait::async_trait]
impl Operation for HealHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle HealHandler, req: {:?}, params: {:?}", req, params);
        let Some(cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };
        info!("cred: {:?}", cred);
        let mut input = req.input;
        let bytes = match input.store_all_unlimited().await {
            Ok(b) => b,
            Err(e) => {
                warn!("get body failed, e: {:?}", e);
                return Err(s3_error!(InvalidRequest, "get body failed"));
            }
        };
        info!("bytes: {:?}", bytes);
        let hip = extract_heal_init_params(&bytes, &req.uri, params)?;
        info!("body: {:?}", hip);

        #[derive(Default)]
        struct HealResp {
            resp_bytes: Vec<u8>,
            _api_err: Option<ec_Error>,
            _err_body: String,
        }

        let heap_path = path_join(&[PathBuf::from(hip.bucket.clone()), PathBuf::from(hip.obj_prefix.clone())]);
        if !hip.client_token.is_empty() && !hip.force_start && !hip.force_stop {
            match GLOBAL_ALlHealState
                .pop_heal_status_json(heap_path.to_str().unwrap_or_default(), &hip.client_token)
                .await
            {
                Ok(b) => {
                    info!("pop_heal_status_json success");
                    return Ok(S3Response::new((StatusCode::OK, Body::from(b))));
                }
                Err(_e) => {
                    info!("pop_heal_status_json failed");
                    return Ok(S3Response::new((StatusCode::INTERNAL_SERVER_ERROR, Body::from(vec![]))));
                }
            }
        }
        let (tx, mut rx) = mpsc::channel(1);
        if hip.force_stop {
            let tx_clone = tx.clone();
            spawn(async move {
                match GLOBAL_ALlHealState
                    .stop_heal_sequence(heap_path.to_str().unwrap_or_default())
                    .await
                {
                    Ok(b) => {
                        let _ = tx_clone
                            .send(HealResp {
                                resp_bytes: b,
                                ..Default::default()
                            })
                            .await;
                    }
                    Err(e) => {
                        let _ = tx_clone
                            .send(HealResp {
                                _api_err: Some(e),
                                ..Default::default()
                            })
                            .await;
                    }
                }
            });
        } else if hip.client_token.is_empty() {
            let nh = Arc::new(new_heal_sequence(&hip.bucket, &hip.obj_prefix, "", hip.hs, hip.force_start));
            let tx_clone = tx.clone();
            spawn(async move {
                match GLOBAL_ALlHealState.launch_new_heal_sequence(nh).await {
                    Ok(b) => {
                        let _ = tx_clone
                            .send(HealResp {
                                resp_bytes: b,
                                ..Default::default()
                            })
                            .await;
                    }
                    Err(e) => {
                        let _ = tx_clone
                            .send(HealResp {
                                _api_err: Some(e),
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

        return Err(s3_error!(NotImplemented));
    }
}

pub struct ListPools {}

#[async_trait::async_trait]
impl Operation for ListPools {
    // GET <endpoint>/<admin-API>/pools/list
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle ListPools");

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let Some(endpoints) = GLOBAL_Endpoints.get() else {
            return Err(s3_error!(NotImplemented));
        };

        if endpoints.legacy() {
            return Err(s3_error!(NotImplemented));
        }

        let mut pools_status = Vec::new();

        for (idx, _) in endpoints.as_ref().iter().enumerate() {
            let state = store.status(idx).await.map_err(to_s3_error)?;

            pools_status.push(state);
        }

        let output = serde_json::to_string(&pools_status)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(output))))
    }
}

#[derive(Debug, Deserialize, Default)]
#[serde(default)]
pub struct StatusPoolQuery {
    pub pool: String,
    #[serde(rename = "by-id")]
    pub by_id: String,
}

pub struct StatusPool {}

#[async_trait::async_trait]
impl Operation for StatusPool {
    // GET <endpoint>/<admin-API>/pools/status?pool=http://server{1...4}/disk{1...4}
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle StatusPool");

        let Some(endpoints) = GLOBAL_Endpoints.get() else {
            return Err(s3_error!(NotImplemented));
        };

        if endpoints.legacy() {
            return Err(s3_error!(NotImplemented));
        }

        let query = {
            if let Some(query) = req.uri.query() {
                let input: StatusPoolQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
                input
            } else {
                StatusPoolQuery::default()
            }
        };

        let is_byid = query.by_id.as_str() == "true";

        let has_idx = {
            if is_byid {
                let a = query.pool.parse::<usize>().unwrap_or_default();
                if a < endpoints.as_ref().len() {
                    Some(a)
                } else {
                    None
                }
            } else {
                endpoints.get_pool_idx(&query.pool)
            }
        };

        let Some(idx) = has_idx else {
            warn!("specified pool {} not found, please specify a valid pool", &query.pool);
            return Err(s3_error!(InvalidArgument));
        };

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let pools_status = store.status(idx).await.map_err(to_s3_error)?;

        let output = serde_json::to_string(&pools_status)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

        Ok(S3Response::new((StatusCode::OK, Body::from(output))))
    }
}

pub struct StartDecommission {}

#[async_trait::async_trait]
impl Operation for StartDecommission {
    // POST <endpoint>/<admin-API>/pools/decommission?pool=http://server{1...4}/disk{1...4}
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle StartDecommission");

        let Some(endpoints) = GLOBAL_Endpoints.get() else {
            return Err(s3_error!(NotImplemented));
        };

        if endpoints.legacy() {
            return Err(s3_error!(NotImplemented));
        }

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        if store.is_decommission_running().await {
            return Err(S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "DecommissionAlreadyRunning".to_string(),
            ));
        }

        // TODO: check IsRebalanceStarted

        let query = {
            if let Some(query) = req.uri.query() {
                let input: StatusPoolQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
                input
            } else {
                StatusPoolQuery::default()
            }
        };
        let is_byid = query.by_id.as_str() == "true";

        let pools: Vec<&str> = query.pool.split(",").collect();
        let mut pools_indices = Vec::with_capacity(pools.len());

        for pool in pools.iter() {
            let idx = {
                if is_byid {
                    pool.parse::<usize>()
                        .map_err(|_e| s3_error!(InvalidArgument, "pool parse failed"))?
                } else {
                    let Some(idx) = endpoints.get_pool_idx(pool) else {
                        return Err(s3_error!(InvalidArgument, "pool parse failed"));
                    };
                    idx
                }
            };

            let mut has_found = None;
            for (i, pool) in store.pools.iter().enumerate() {
                if i == idx {
                    has_found = Some(pool.clone());
                    break;
                }
            }

            let Some(_p) = has_found else {
                return Err(s3_error!(InvalidArgument));
            };

            pools_indices.push(idx);
        }

        if !pools_indices.is_empty() {
            store.decommission(pools_indices).await.map_err(to_s3_error)?;
        }

        Ok(S3Response::new((StatusCode::OK, Body::default())))
    }
}

pub struct CancelDecommission {}

#[async_trait::async_trait]
impl Operation for CancelDecommission {
    // POST <endpoint>/<admin-API>/pools/cancel?pool=http://server{1...4}/disk{1...4}
    async fn call(&self, req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle CancelDecommission");

        let Some(endpoints) = GLOBAL_Endpoints.get() else {
            return Err(s3_error!(NotImplemented));
        };

        if endpoints.legacy() {
            return Err(s3_error!(NotImplemented));
        }

        let query = {
            if let Some(query) = req.uri.query() {
                let input: StatusPoolQuery =
                    from_bytes(query.as_bytes()).map_err(|_e| s3_error!(InvalidArgument, "get body failed"))?;
                input
            } else {
                StatusPoolQuery::default()
            }
        };

        let is_byid = query.by_id.as_str() == "true";

        let has_idx = {
            if is_byid {
                let a = query.pool.parse::<usize>().unwrap_or_default();
                if a < endpoints.as_ref().len() {
                    Some(a)
                } else {
                    None
                }
            } else {
                endpoints.get_pool_idx(&query.pool)
            }
        };

        let Some(idx) = has_idx else {
            warn!("specified pool {} not found, please specify a valid pool", &query.pool);
            return Err(s3_error!(InvalidArgument));
        };

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        store.decommission_cancel(idx).await.map_err(to_s3_error)?;

        Ok(S3Response::new((StatusCode::OK, Body::default())))
    }
}

pub struct RebalanceStart {}

#[async_trait::async_trait]
impl Operation for RebalanceStart {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStart");

        return Err(s3_error!(NotImplemented));
    }
}

// RebalanceStatus
pub struct RebalanceStatus {}

#[async_trait::async_trait]
impl Operation for RebalanceStatus {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStatus");

        return Err(s3_error!(NotImplemented));
    }
}
// RebalanceStop
pub struct RebalanceStop {}

#[async_trait::async_trait]
impl Operation for RebalanceStop {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle RebalanceStop");

        return Err(s3_error!(NotImplemented));
    }
}

#[cfg(test)]
mod test {
    use ecstore::heal::heal_commands::HealOpts;

    #[test]
    fn test_decode() {
        let b = b"{\"recursive\":false,\"dryRun\":false,\"remove\":false,\"recreate\":false,\"scanMode\":1,\"updateParity\":false,\"nolock\":false}";
        let s: HealOpts = serde_urlencoded::from_bytes(b).unwrap();
        println!("{:?}", s);
    }
}
