use super::router::Operation;
use crate::auth::check_key_valid;
use crate::auth::get_condition_values;
use crate::auth::get_session_token;
use bytes::Bytes;
use common::error::Error as ec_Error;
use ecstore::admin_server_info::get_server_info;
use ecstore::bucket::versioning_sys::BucketVersioningSys;
use ecstore::global::GLOBAL_ALlHealState;
use ecstore::heal::data_usage::load_data_usage_from_backend;
use ecstore::heal::heal_commands::HealOpts;
use ecstore::heal::heal_ops::new_heal_sequence;
use ecstore::metrics_realtime::{collect_local_metrics, CollectMetricsOpts, MetricType};
use ecstore::new_object_layer_fn;
use ecstore::peer::is_reserved_or_invalid_bucket;
use ecstore::pools::{get_total_usable_capacity, get_total_usable_capacity_free};
use ecstore::store::is_valid_object_prefix;
use ecstore::store_api::BucketOptions;
use ecstore::store_api::StorageAPI;
use ecstore::utils::path::path_join;
use futures::{Stream, StreamExt};
use http::{HeaderMap, Uri};
use hyper::StatusCode;
use iam::get_global_action_cred;
use iam::store::MappedPolicy;
use madmin::metrics::RealtimeMetrics;
use madmin::utils::parse_duration;
use matchit::Params;
use policy::policy::action::Action;
use policy::policy::action::S3Action;
use policy::policy::default::DEFAULT_POLICIES;
use policy::policy::Args;
use policy::policy::BucketPolicy;
use s3s::header::CONTENT_TYPE;
use s3s::stream::{ByteStream, DynByteStream};
use s3s::{s3_error, Body, S3Error, S3Request, S3Response, S3Result};
use s3s::{S3ErrorCode, StdError};
use serde::{Deserialize, Serialize};
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
pub mod policys;
pub mod pools;
pub mod rebalance;
pub mod service_account;
pub mod sts;
pub mod trace;
pub mod user;

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
        let Some(store) = new_object_layer_fn() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init".to_string()));
        };

        let Some(input_cred) = req.credentials else {
            return Err(s3_error!(InvalidRequest, "get cred failed"));
        };

        let (cred, owner) =
            check_key_valid(get_session_token(&req.uri, &req.headers).unwrap_or_default(), &input_cred.access_key).await?;

        let Ok(iam_store) = iam::get() else { return Err(s3_error!(InvalidRequest, "iam not init")) };

        let default_claims = HashMap::new();
        let claims = cred.claims.as_ref().unwrap_or(&default_claims);

        let cred_clone = cred.clone();
        let conditions = get_condition_values(&req.headers, &cred_clone);
        let cred_clone = Arc::new(cred_clone);
        let conditions = Arc::new(conditions);

        let is_allow = Box::new({
            let iam_clone = Arc::clone(&iam_store);
            let cred_clone = Arc::clone(&cred_clone);
            let conditions = Arc::clone(&conditions);
            move |name: String| {
                let iam_clone = Arc::clone(&iam_clone);
                let cred_clone = Arc::clone(&cred_clone);
                let conditions = Arc::clone(&conditions);
                async move {
                    let (mut rd, mut wr) = (false, false);
                    if !iam_clone
                        .is_allowed(&Args {
                            account: &cred_clone.access_key,
                            groups: &cred_clone.groups,
                            action: Action::S3Action(S3Action::ListBucketAction),
                            bucket: &name,
                            conditions: &conditions,
                            is_owner: owner,
                            object: "",
                            claims,
                            deny_only: false,
                        })
                        .await
                    {
                        rd = true
                    }

                    if !iam_clone
                        .is_allowed(&Args {
                            account: &cred_clone.access_key,
                            groups: &cred_clone.groups,
                            action: Action::S3Action(S3Action::GetBucketLocationAction),
                            bucket: &name,
                            conditions: &conditions,
                            is_owner: owner,
                            object: "",
                            claims,
                            deny_only: false,
                        })
                        .await
                    {
                        rd = true
                    }

                    if !iam_clone
                        .is_allowed(&Args {
                            account: &cred_clone.access_key,
                            groups: &cred_clone.groups,
                            action: Action::S3Action(S3Action::PutObjectAction),
                            bucket: &name,
                            conditions: &conditions,
                            is_owner: owner,
                            object: "",
                            claims,
                            deny_only: false,
                        })
                        .await
                    {
                        wr = true
                    }

                    (rd, wr)
                }
            }
        });

        let account_name = if cred.is_temp() || cred.is_service_account() {
            cred.parent_user.clone()
        } else {
            cred.access_key.clone()
        };

        let claims_args = Args {
            account: "",
            groups: &None,
            action: Action::None,
            bucket: "",
            conditions: &HashMap::new(),
            is_owner: false,
            object: "",
            claims,
            deny_only: false,
        };

        let role_arn = claims_args.get_role_arn();

        // TODO: get_policies_from_claims(claims);

        let Some(admin_cred) = get_global_action_cred() else {
            return Err(S3Error::with_message(
                S3ErrorCode::InternalError,
                "get_global_action_cred failed".to_string(),
            ));
        };

        let mut effective_policy: policy::policy::Policy = Default::default();

        if account_name == admin_cred.access_key {
            for (name, p) in DEFAULT_POLICIES.iter() {
                if *name == "consoleAdmin" {
                    effective_policy = p.clone();
                    break;
                }
            }
        } else if let Some(arn) = role_arn {
            let (_, policy_name) = iam_store
                .get_role_policy(arn)
                .await
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

            let policies = MappedPolicy::new(&policy_name).to_slice();
            effective_policy = iam_store.get_combined_policy(&policies).await;
        } else {
            let policies = iam_store
                .policy_db_get(&account_name, &cred.groups)
                .await
                .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, format!("get policy failed: {}", e)))?;

            effective_policy = iam_store.get_combined_policy(&policies).await;
        };

        let policy_str = serde_json::to_string(&effective_policy)
            .map_err(|_e| S3Error::with_message(S3ErrorCode::InternalError, "parse policy failed"))?;

        let mut account_info = madmin::AccountInfo {
            account_name,
            server: store.backend_info().await,
            policy: serde_json::Value::String(policy_str),
            ..Default::default()
        };

        // TODO: bucket policy
        let buckets = store
            .list_bucket(&BucketOptions {
                cached: true,
                ..Default::default()
            })
            .await
            .map_err(|e| S3Error::with_message(S3ErrorCode::InternalError, e.to_string()))?;

        for bucket in buckets.iter() {
            let (rd, wr) = is_allow(bucket.name.clone()).await;
            if rd || wr {
                // TODO: BucketQuotaSys
                // TODO: other attributes
                account_info.buckets.push(madmin::BucketAccessInfo {
                    name: bucket.name.clone(),
                    details: Some(madmin::BucketDetails {
                        versioning: BucketVersioningSys::enabled(bucket.name.as_str()).await,
                        versioning_suspended: BucketVersioningSys::suspended(bucket.name.as_str()).await,
                        ..Default::default()
                    }),
                    created: bucket.created,
                    access: madmin::AccountAccess { read: rd, write: wr },
                    ..Default::default()
                });
            }
        }

        let data = serde_json::to_vec(&account_info)
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

        let mut info = load_data_usage_from_backend(store.clone()).await.map_err(|e| {
            error!("load_data_usage_from_backend failed {:?}", e);
            s3_error!(InternalError, "load_data_usage_from_backend failed")
        })?;

        let sinfo = store.storage_info().await;
        info.total_capacity = get_total_usable_capacity(&sinfo.disks, &sinfo) as u64;
        info.total_free_capacity = get_total_usable_capacity_free(&sinfo.disks, &sinfo) as u64;
        if info.total_capacity > info.total_free_capacity {
            info.total_used_capacity = info.total_capacity - info.total_free_capacity;
        }

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
