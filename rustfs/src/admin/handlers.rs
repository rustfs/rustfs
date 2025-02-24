use super::router::Operation;
use crate::storage::error::to_s3_error;
use bytes::Bytes;
use ecstore::admin_server_info::get_server_info;
use ecstore::bucket::policy::action::{Action, ActionSet};
use ecstore::bucket::policy::bucket_policy::{BPStatement, BucketPolicy};
use ecstore::bucket::policy::effect::Effect;
use ecstore::bucket::policy::resource::{Resource, ResourceSet};
use ecstore::error::Error as ec_Error;
use ecstore::global::GLOBAL_ALlHealState;
use ecstore::heal::data_usage::load_data_usage_from_backend;
use ecstore::heal::heal_commands::HealOpts;
use ecstore::heal::heal_ops::new_heal_sequence;
use ecstore::metrics_realtime::{collect_local_metrics, CollectMetricsOpts, MetricType};
use ecstore::new_object_layer_fn;
use ecstore::peer::is_reserved_or_invalid_bucket;
use ecstore::store::is_valid_object_prefix;
use ecstore::store_api::StorageAPI;
use ecstore::utils::crypto::base64_encode;
use ecstore::utils::path::path_join;
use ecstore::GLOBAL_Endpoints;
use futures::{Stream, StreamExt};
use http::{HeaderMap, Uri};
use hyper::StatusCode;
use iam::auth::get_claims_from_token_with_secret;
use iam::error::Error as IamError;
use iam::policy::Policy;
use iam::sys::SESSION_POLICY_NAME;
use iam::{auth, get_global_action_cred};
use madmin::metrics::RealtimeMetrics;
use madmin::utils::parse_duration;
use matchit::Params;
use s3s::header::CONTENT_TYPE;
use s3s::stream::{ByteStream, DynByteStream};
use s3s::{s3_error, Body, S3Error, S3Request, S3Response, S3Result};
use s3s::{S3ErrorCode, StdError};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use serde_urlencoded::from_bytes;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration as std_Duration;
use tokio::sync::mpsc::{self};
use tokio::time::interval;
use tokio::{select, spawn};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, warn};

pub mod group;
pub mod policy;
pub mod service_account;
pub mod sts;
pub mod trace;
pub mod user;

// check_key_valid get auth.cred
pub async fn check_key_valid(security_token: Option<String>, ak: &str) -> S3Result<(auth::Credentials, bool)> {
    let Some(mut cred) = get_global_action_cred() else {
        return Err(S3Error::with_message(
            S3ErrorCode::InternalError,
            format!("get_global_action_cred {:?}", IamError::IamSysNotInitialized),
        ));
    };

    let sys_cred = cred.clone();

    // warn!("check_key_valid cred {:?}, as: {:?}", &cred, &ak);
    if cred.access_key != ak {
        let Ok(iam_store) = iam::get() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                format!("check_key_valid {:?}", IamError::IamSysNotInitialized),
            ));
        };

        let (u, ok) = iam_store
            .check_key(ak)
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("check claims failed1 {}", e)))?;

        if !ok {
            if let Some(u) = u {
                if u.credentials.status == "off" {
                    return Err(s3_error!(InvalidRequest, "ErrAccessKeyDisabled"));
                }
            }

            return Err(s3_error!(InvalidRequest, "check key failed"));
        }

        let Some(u) = u else {
            return Err(s3_error!(InvalidRequest, "check key failed"));
        };

        cred = u.credentials;
    }

    // warn!("check_key_valid cred {:?}", &cred);
    // warn!("check_key_valid security_token {:?}", &security_token);

    let claims = check_claims_from_token(&security_token.unwrap_or_default(), &cred)
        .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("check claims failed {}", e)))?;

    cred.claims = if !claims.is_empty() { Some(claims) } else { None };

    let mut owner = sys_cred.access_key == cred.access_key || cred.parent_user == sys_cred.access_key;

    // permitRootAccess
    if let Some(claims) = &cred.claims {
        if claims.contains_key(SESSION_POLICY_NAME) {
            owner = false
        }
    }

    Ok((cred, owner))
}

pub fn check_claims_from_token(token: &str, cred: &auth::Credentials) -> S3Result<HashMap<String, Value>> {
    if !token.is_empty() && cred.access_key.is_empty() {
        return Err(s3_error!(InvalidRequest, "no access key"));
    }

    if token.is_empty() && cred.is_temp() && !cred.is_service_account() {
        return Err(s3_error!(InvalidRequest, "invalid token"));
    }

    if !token.is_empty() && !cred.is_temp() {
        return Err(s3_error!(InvalidRequest, "invalid token"));
    }

    if !cred.is_service_account() && cred.is_temp() && token != cred.session_token {
        return Err(s3_error!(InvalidRequest, "invalid token"));
    }

    if cred.is_temp() && cred.is_expired() {
        return Err(s3_error!(InvalidRequest, "invalid access key is temp and expired"));
    }

    let Some(sys_cred) = get_global_action_cred() else {
        return Err(s3_error!(InternalError, "action cred not init"));
    };

    let mut secret = sys_cred.secret_key;

    // TODO: REPLICATION

    let mut token = token;

    if cred.is_service_account() {
        token = cred.session_token.as_str();
        secret = cred.secret_key.clone();
    }

    if !token.is_empty() {
        let claims: HashMap<String, Value> =
            get_claims_from_token_with_secret(token, &secret).map_err(|_e| s3_error!(InvalidRequest, "invalid token"))?;
        return Ok(claims);
    }

    Ok(HashMap::new())
}

pub fn get_session_token(hds: &HeaderMap) -> Option<String> {
    hds.get("x-amz-security-token")
        .map(|v| v.to_str().unwrap_or_default().to_string())
}

pub fn populate_session_policy(claims: &mut HashMap<String, Value>, policy: &str) -> S3Result<()> {
    if !policy.is_empty() {
        let session_policy = Policy::parse_config(policy.as_bytes())
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("parse policy err {}", e)))?;
        if session_policy.version.is_empty() {
            return Err(s3_error!(InvalidRequest, "invalid policy"));
        }

        let policy_buf = serde_json::to_vec(&session_policy)
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("marshal policy err {}", e)))?;

        if policy_buf.len() > 2048 {
            return Err(s3_error!(InvalidRequest, "policy too large"));
        }

        claims.insert(SESSION_POLICY_NAME.to_string(), serde_json::Value::String(base64_encode(&policy_buf)));
    }

    Ok(())
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "PascalCase", default)]
pub struct AccountInfo {
    pub account_name: String,
    pub server: madmin::BackendInfo,
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

        let data = serde_json::to_vec(&info)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
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

        let info = get_server_info(true).await;

        let data = serde_json::to_vec(&info)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse serverInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
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

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        // TODO:getAggregatedBackgroundHealState

        let info = store.storage_info().await;

        let data = serde_json::to_vec(&info)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

pub struct DataUsageInfoHandler {}

#[async_trait::async_trait]
impl Operation for DataUsageInfoHandler {
    async fn call(&self, _req: S3Request<Body>, _params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        warn!("handle DataUsageInfoHandler");

        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let info = load_data_usage_from_backend(store).await.map_err(|e| {
            error!("load_data_usage_from_backend failed {:?}", e);
            s3_error!(InternalError, "load_data_usage_from_backend failed")
        })?;

        let data = serde_json::to_vec(&info)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse DataUsageInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct MetricsParams {
    disks: String,
    hosts: String,
    #[serde(rename = "interval")]
    tick: String,
    n: u64,
    types: u32,
    #[serde(rename = "by-disk")]
    by_disk: String,
    #[serde(rename = "by-host")]
    by_host: String,
    #[serde(rename = "by-jobID")]
    by_job_id: String,
    #[serde(rename = "by-depID")]
    by_dep_id: String,
}

impl Default for MetricsParams {
    fn default() -> Self {
        Self {
            disks: Default::default(),
            hosts: Default::default(),
            tick: Default::default(),
            n: u64::MAX,
            types: Default::default(),
            by_disk: Default::default(),
            by_host: Default::default(),
            by_job_id: Default::default(),
            by_dep_id: Default::default(),
        }
    }
}

fn extract_metrics_init_params(uri: &Uri) -> MetricsParams {
    let mut mp = MetricsParams::default();
    if let Some(query) = uri.query() {
        let params: Vec<&str> = query.split('&').collect();
        for param in params {
            let mut parts = param.split('=');
            if let Some(key) = parts.next() {
                if key == "disks" {
                    if let Some(value) = parts.next() {
                        mp.disks = value.to_string();
                    }
                }
                if key == "hosts" {
                    if let Some(value) = parts.next() {
                        mp.hosts = value.to_string();
                    }
                }
                if key == "interval" {
                    if let Some(value) = parts.next() {
                        mp.tick = value.to_string();
                    }
                }
                if key == "n" {
                    if let Some(value) = parts.next() {
                        mp.n = value.parse::<u64>().unwrap_or(u64::MAX);
                    }
                }
                if key == "types" {
                    if let Some(value) = parts.next() {
                        mp.types = value.parse::<u32>().unwrap_or_default();
                    }
                }
                if key == "by-disk" {
                    if let Some(value) = parts.next() {
                        mp.by_disk = value.to_string();
                    }
                }
                if key == "by-host" {
                    if let Some(value) = parts.next() {
                        mp.by_host = value.to_string();
                    }
                }
                if key == "by-jobID" {
                    if let Some(value) = parts.next() {
                        mp.by_job_id = value.to_string();
                    }
                }
                if key == "by-depID" {
                    if let Some(value) = parts.next() {
                        mp.by_dep_id = value.to_string();
                    }
                }
            }
        }
    }
    mp
}

struct MetricsStream {
    inner: ReceiverStream<Result<Bytes, StdError>>,
}

impl Stream for MetricsStream {
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        info!("MetricsStream poll_next");
        let this = Pin::into_inner(self);
        this.inner.poll_next_unpin(cx)
    }
}

impl ByteStream for MetricsStream {}

pub struct MetricsHandler {}

#[async_trait::async_trait]
impl Operation for MetricsHandler {
    async fn call(&self, req: S3Request<Body>, params: Params<'_, '_>) -> S3Result<S3Response<(StatusCode, Body)>> {
        info!("handle MetricsHandler, req: {:?}, params: {:?}", req, params);
        let Some(cred) = req.credentials else { return Err(s3_error!(InvalidRequest, "get cred failed")) };
        info!("cred: {:?}", cred);

        let mp = extract_metrics_init_params(&req.uri);
        info!("mp: {:?}", mp);

        let tick = match parse_duration(&mp.tick) {
            Ok(i) => i,
            Err(_) => std_Duration::from_secs(1),
        };

        let mut n = mp.n;
        if n == 0 {
            n = u64::MAX;
        }

        let types = if mp.types != 0 {
            MetricType::new(mp.types)
        } else {
            MetricType::ALL
        };

        let disks = mp.disks.split(",").map(String::from).collect::<Vec<String>>();
        let by_disk = mp.by_disk == "true";
        let mut disk_map = HashSet::new();
        if !disks.is_empty() && !disks[0].is_empty() {
            for d in disks.iter() {
                if !d.is_empty() {
                    disk_map.insert(d.to_string());
                }
            }
        }

        let job_id = mp.by_job_id;
        let hosts = mp.hosts.split(",").map(String::from).collect::<Vec<String>>();
        let by_host = mp.by_host == "true";
        let mut host_map = HashSet::new();
        if !hosts.is_empty() && !hosts[0].is_empty() {
            for d in hosts.iter() {
                if !d.is_empty() {
                    host_map.insert(d.to_string());
                }
            }
        }

        let d_id = mp.by_dep_id;
        let mut interval = interval(tick);

        let opts = CollectMetricsOpts {
            hosts: host_map,
            disks: disk_map,
            job_id,
            dep_id: d_id,
        };
        let (tx, rx) = mpsc::channel(10);
        let in_stream: DynByteStream = Box::pin(MetricsStream {
            inner: ReceiverStream::new(rx),
        });
        let body = Body::from(in_stream);
        tokio::spawn(async move {
            while n > 0 {
                info!("loop, n: {n}");
                let mut m = RealtimeMetrics::default();
                let m_local = collect_local_metrics(types, &opts).await;
                m.merge(m_local);

                if !by_host {
                    m.by_host = HashMap::new();
                }
                if !by_disk {
                    m.by_disk = HashMap::new();
                }

                m.finally = n <= 1;

                // todo write resp
                match serde_json::to_vec(&m) {
                    Ok(re) => {
                        info!("got metrics, send it to client, m: {m:?}");
                        let _ = tx.send(Ok(Bytes::from(re))).await;
                    }
                    Err(e) => {
                        error!("MetricsHandler: json encode failed, err: {:?}", e);
                        return;
                    }
                }

                n -= 1;
                if n == 0 {
                    break;
                }

                select! {
                    _ = tx.closed() => { return; }
                    _ = interval.tick() => {}
                }
            }
        });

        Ok(S3Response::new((StatusCode::OK, body)))
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

        let heal_path = path_join(&[PathBuf::from(hip.bucket.clone()), PathBuf::from(hip.obj_prefix.clone())]);
        if !hip.client_token.is_empty() && !hip.force_start && !hip.force_stop {
            match GLOBAL_ALlHealState
                .pop_heal_status_json(heal_path.to_str().unwrap_or_default(), &hip.client_token)
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
                    .stop_heal_sequence(heal_path.to_str().unwrap_or_default())
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

        let data = serde_json::to_vec(&pools_status)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
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

        let data = serde_json::to_vec(&pools_status)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse accountInfo failed"))?;

        let mut header = HeaderMap::new();
        header.insert(CONTENT_TYPE, "application/json".parse().unwrap());

        Ok(S3Response::with_headers((StatusCode::OK, Body::from(data)), header))
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

    #[ignore] // FIXME: failed in github actions
    #[test]
    fn test_decode() {
        let b = b"{\"recursive\":false,\"dryRun\":false,\"remove\":false,\"recreate\":false,\"scanMode\":1,\"updateParity\":false,\"nolock\":false}";
        let s: HealOpts = serde_urlencoded::from_bytes(b).unwrap();
        println!("{:?}", s);
    }
}
